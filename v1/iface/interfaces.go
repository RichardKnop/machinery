package iface

import (
	"context"

	backendsiface "github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/backends/result"
	brokersiface "github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Server for sending and processing tasks
//go:generate go run github.com/vektra/mockery/cmd/mockery -name Server
type Server interface {
	// NewWorker(consumerTag string, concurrency int) Worker
	// NewCustomQueueWorker(consumerTag string, concurrency int, queue string) Worker
	GetBroker() brokersiface.Broker
	SetBroker(broker brokersiface.Broker)
	GetBackend() backendsiface.Backend
	SetBackend(backend backendsiface.Backend)
	GetConfig() *config.Config
	SetConfig(cnf *config.Config)
	SetPreTaskHandler(handler func(*tasks.Signature))
	RegisterTasks(namedTaskFuncs map[string]interface{}) error
	RegisterTask(name string, taskFunc interface{}) error
	IsTaskRegistered(name string) bool
	GetRegisteredTask(name string) (interface{}, error)
	SendTaskWithContext(ctx context.Context, signature *tasks.Signature) (*result.AsyncResult, error)
	SendTask(signature *tasks.Signature) (*result.AsyncResult, error)
	SendChainWithContext(ctx context.Context, chain *tasks.Chain) (*result.ChainAsyncResult, error)
	SendChain(chain *tasks.Chain) (*result.ChainAsyncResult, error)
	SendGroupWithContext(ctx context.Context, group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error)
	SendGroup(group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error)
	SendChordWithContext(ctx context.Context, chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error)
	SendChord(chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error)
	GetRegisteredTaskNames() []string
}

// Worker represents a single worker process
//go:generate go run github.com/vektra/mockery/cmd/mockery -name Worker
type Worker interface {
	Launch() error
	LaunchAsync(errorsChan chan<- error)
	CustomQueue() string
	Quit()
	Process(signature *tasks.Signature) error
	SetErrorHandler(handler func(err error))
	SetPreTaskHandler(handler func(*tasks.Signature))
	SetPostTaskHandler(handler func(*tasks.Signature))
	SetPreConsumeHandler(handler func(Worker) bool)
	GetServer() *Server
	PreConsumeHandler() bool
}
