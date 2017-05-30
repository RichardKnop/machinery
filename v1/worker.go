package machinery

import (
	"fmt"
	"strings"

	"os"
	"os/signal"
	"syscall"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Worker represents a single worker process
type Worker struct {
	server      *Server
	ConsumerTag string
}

// Launch starts a new worker process. The worker subscribes
// to the default queue and processes incoming registered tasks
func (worker *Worker) Launch() error {
	cnf := worker.server.GetConfig()
	broker := worker.server.GetBroker()

	log.INFO.Printf("Launching a worker with the following settings:")
	log.INFO.Printf("- Broker: %s", cnf.Broker)
	log.INFO.Printf("- DefaultQueue: %s", cnf.DefaultQueue)
	log.INFO.Printf("- ResultBackend: %s", cnf.ResultBackend)
	if cnf.AMQP != nil {
		log.INFO.Printf("- AMQP: %s", cnf.AMQP.Exchange)
		log.INFO.Printf("  - Exchange: %s", cnf.AMQP.Exchange)
		log.INFO.Printf("  - ExchangeType: %s", cnf.AMQP.ExchangeType)
		log.INFO.Printf("  - BindingKey: %s", cnf.AMQP.BindingKey)
		log.INFO.Printf("  - PrefetchCount: %d", cnf.AMQP.PrefetchCount)
	}

	errorsChan := make(chan error)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			retry, err := broker.StartConsuming(worker.ConsumerTag, worker)

			if retry {
				log.WARNING.Printf("Going to retry launching the worker. Error: %v", err)
			} else {
				errorsChan <- err // stop the goroutine
				return
			}
		}
	}()

	go func() {
		err := fmt.Errorf("Signal received: %v. Quitting the worker", <-sig)
		log.WARNING.Print(err.Error())
		worker.Quit()
		errorsChan <- err
	}()

	return <-errorsChan
}

// Quit tears down the running worker process
func (worker *Worker) Quit() {
	worker.server.GetBroker().StopConsuming()
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *tasks.Signature) error {
	// If the task is not registered with this worker, do not continue
	// but only return nil as we do not want to restart the worker process
	if !worker.server.IsTaskRegistered(signature.Name) {
		return nil
	}

	taskFunc, err := worker.server.GetRegisteredTask(signature.Name)
	if err != nil {
		return nil
	}

	backend := worker.server.GetBackend()

	// Update task state to RECEIVED
	if err = backend.SetStateReceived(signature); err != nil {
		return fmt.Errorf("Set State Received: %v", err)
	}

	// Prepare task for processing
	task, err := tasks.New(taskFunc, signature.Args)
	if err != nil {
		worker.finalizeError(signature, err)
		return err
	}

	// Update task state to STARTED
	if err = backend.SetStateStarted(signature); err != nil {
		return fmt.Errorf("Set State Started: %v", err)
	}

	// Call the task
	results, err := task.Call()
	if err != nil {
		return worker.finalizeError(signature, err)
	}

	return worker.finalizeSuccess(signature, results)
}

// Task succeeded, update state and trigger success callbacks
func (worker *Worker) finalizeSuccess(signature *tasks.Signature, taskResults []*tasks.TaskResult) error {
	// Update task state to SUCCESS
	backend := worker.server.GetBackend()

	if err := backend.SetStateSuccess(signature, taskResults); err != nil {
		return fmt.Errorf("Set State Success: %v", err)
	}

	debugResults := make([]string, len(taskResults))
	for i, taskResult := range taskResults {
		debugResults[i] = fmt.Sprintf("%v", taskResult.Value)
	}
	log.INFO.Printf("Processed %s. Results = [%v]", signature.UUID, strings.Join(debugResults, ", "))

	// Trigger success callbacks
	for _, successTask := range signature.OnSuccess {
		if signature.Immutable == false {
			// Pass results of the task to success callbacks
			args := make([]tasks.Arg, 0)
			for _, taskResult := range taskResults {
				args = append([]tasks.Arg{{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				}}, successTask.Args...)
			}
			successTask.Args = args
		}

		worker.server.SendTask(successTask)
	}

	// If the task was not part of a group, just return
	if signature.GroupUUID == "" {
		return nil
	}

	// Check if all task in the group has completed
	groupCompleted, err := worker.server.GetBackend().GroupCompleted(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return fmt.Errorf("GroupCompleted: %v", err)
	}
	// If the group has not yet completed, just return
	if !groupCompleted {
		return nil
	}

	// Defer purging of group meta queue if we are using AMQP backend
	if worker.hasAMQPBackend() {
		defer worker.server.backend.PurgeGroupMeta(signature.GroupUUID)
	}

	// There is no chord callback, just return
	if signature.ChordCallback == nil {
		return nil
	}

	// Trigger chord callback
	shouldTrigger, err := worker.server.backend.TriggerChord(signature.GroupUUID)
	if err != nil {
		return fmt.Errorf("TriggerChord: %v", err)
	}

	// Chord has already been triggered
	if !shouldTrigger {
		return nil
	}

	// Get task states
	taskStates, err := worker.server.GetBackend().GroupTaskStates(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return nil
	}

	// Append group tasks' return values to chord task if it's not immutable
	for _, taskState := range taskStates {
		if !taskState.IsSuccess() {
			return nil
		}

		if signature.ChordCallback.Immutable == false {
			// Pass results of the task to the chord callback
			for _, taskResult := range taskState.Results {
				signature.ChordCallback.Args = append(signature.ChordCallback.Args, tasks.Arg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}
	}

	// Send the chord task
	_, err = worker.server.SendTask(signature.ChordCallback)
	if err != nil {
		return err
	}

	return nil
}

// Task failed, update state and trigger error callbacks
func (worker *Worker) finalizeError(signature *tasks.Signature, err error) error {
	// Update task state to FAILURE
	backend := worker.server.GetBackend()
	if err1 := backend.SetStateFailure(signature, err.Error()); err1 != nil {
		return fmt.Errorf("Set State Failure: %v", err1)
	}

	log.ERROR.Printf("Failed processing %s. Error = %v", signature.UUID, err)

	// Trigger error callbacks
	for _, errorTask := range signature.OnError {
		// Pass error as a first argument to error callbacks
		args := append([]tasks.Arg{{
			Type:  "string",
			Value: err.Error(),
		}}, errorTask.Args...)
		errorTask.Args = args
		worker.server.SendTask(errorTask)
	}

	if signature.RetryCount > 0 {
		signature.RetryCount--
	}

	return nil
}

// Returns true if the worker uses AMQP backend
func (worker *Worker) hasAMQPBackend() bool {
	_, ok := worker.server.backend.(*backends.AMQPBackend)
	return ok
}
