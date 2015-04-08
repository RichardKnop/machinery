package lib

import (
	"fmt"
	"encoding/json"
	"github.com/streadway/amqp"
)

type App struct {
	BrokerURL string
	DefaultQueue string
	registeredTasks []string
}

func InitApp(configMap map[interface{}]interface{}) *App {
	brokerURL, ok := configMap["broker_url"].(string)
	if ok != true {
		FailOnError(ArgumentNotString{}, 
			"broker_url must be string")
	}

	defaultQueue, ok := configMap["default_queue"].(string)
	if ok != true {
		FailOnError(ArgumentNotString{}, 
			"default_queue must be string")
	}

	return &App{
		BrokerURL: brokerURL,
		DefaultQueue: defaultQueue,
		registeredTasks: make([]string, 5),
	}
}

func (app *App) SendTask(taskName string) {
	conn, ch, q := Connect(app)
	defer conn.Close()
	defer ch.Close()

	message := fmt.Sprintf("{\"name\": \"%s\"}", taskName)
	encodedMsgBody, err := json.Marshal(message)
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(encodedMsgBody),
		})
	FailOnError(err, "Failed to publish a message")
}