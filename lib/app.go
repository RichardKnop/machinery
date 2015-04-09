package lib

import (
	"encoding/json"
	"github.com/streadway/amqp"
)

type App struct {
	BrokerURL string
	DefaultQueue string
	registeredTasks map[string]Task
}

func InitApp(configMap map[string]string) *App {
	return &App{
		BrokerURL: configMap["broker_url"],
		DefaultQueue: configMap["default_queue"],
	}
}

func (app *App) RegisterTasks(tasks map[string]Task) {
	app.registeredTasks = tasks
}

func (app *App) GetRegisteredTask(name string) Task {
	return app.registeredTasks[name]
}

func (app *App) SendTask(name string, kwargs map[string]interface{}) {
	conn, ch, q := Connect(app)
	defer conn.Close()
	defer ch.Close()

	message := make(map[string]interface{})
	message["name"] = name
	message["kwargs"] = kwargs
	encodedMessage, err := json.Marshal(message)

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(encodedMessage),
		})
	FailOnError(err, "Failed to publish a message")
}