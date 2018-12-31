package config

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/mongodb/mongo-go-driver/mongo"
)

const (
	// DefaultResultsExpireIn is a default time used to expire task states and group metadata from the backend
	DefaultResultsExpireIn = 24 * 3600
)

var (
	// Start with sensible default values
	defaultCnf = &Config{
		Broker:          "amqp://guest:guest@localhost:5672/",
		DefaultQueue:    "machinery_tasks",
		ResultBackend:   "amqp://guest:guest@localhost:5672/",
		ResultsExpireIn: DefaultResultsExpireIn,
		AMQP: &AMQPConfig{
			Exchange:      "machinery_exchange",
			ExchangeType:  "direct",
			BindingKey:    "machinery_task",
			PrefetchCount: 3,
		},
		DynamoDB: &DynamoDBConfig{
			TaskStatesTable: "task_states",
			GroupMetasTable: "group_metas",
		},
		Redis: &RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			DelayedTasksPollPeriod: 20,
		},
		GCPPubSub: &GCPPubSubConfig{
			Client: nil,
		},
	}

	reloadDelay = time.Second * 10
)

// Config holds all configuration for our program
type Config struct {
	Broker          string           `yaml:"broker" envconfig:"BROKER"`
	DefaultQueue    string           `yaml:"default_queue" envconfig:"DEFAULT_QUEUE"`
	ResultBackend   string           `yaml:"result_backend" envconfig:"RESULT_BACKEND"`
	ResultsExpireIn int              `yaml:"results_expire_in" envconfig:"RESULTS_EXPIRE_IN"`
	AMQP            *AMQPConfig      `yaml:"amqp"`
	SQS             *SQSConfig       `yaml:"sqs"`
	Redis           *RedisConfig     `yaml:"redis"`
	GCPPubSub       *GCPPubSubConfig `yaml:"-" ignored:"true"`
	MongoDB         *MongoDBConfig   `yamk:"-" ignored:"ture"`
	TLSConfig       *tls.Config
	// NoUnixSignals - when set disables signal handling in machinery
	NoUnixSignals bool            `yaml:"no_unix_signals" envconfig:"NO_UNIX_SIGNALS"`
	DynamoDB      *DynamoDBConfig `yaml:"dynamodb"`
}

// QueueBindingArgs arguments which are used when binding to the exchange
type QueueBindingArgs map[string]interface{}

// AMQPConfig wraps RabbitMQ related configuration
type AMQPConfig struct {
	Exchange         string           `yaml:"exchange" envconfig:"AMQP_EXCHANGE"`
	ExchangeType     string           `yaml:"exchange_type" envconfig:"AMQP_EXCHANGE_TYPE"`
	QueueBindingArgs QueueBindingArgs `yaml:"queue_binding_args" envconfig:"AMQP_QUEUE_BINDING_ARGS"`
	BindingKey       string           `yaml:"binding_key" envconfig:"AMQP_BINDING_KEY"`
	PrefetchCount    int              `yaml:"prefetch_count" envconfig:"AMQP_PREFETCH_COUNT"`
}

// DynamoDBConfig wraps DynamoDB related configuration
type DynamoDBConfig struct {
	Client          *dynamodb.DynamoDB
	TaskStatesTable string `yaml:"task_states_table" envconfig:"TASK_STATES_TABLE"`
	GroupMetasTable string `yaml:"group_metas_table" envconfig:"GROUP_METAS_TABLE"`
}

// SQSConfig wraps SQS related configuration
type SQSConfig struct {
	Client          *sqs.SQS
	WaitTimeSeconds int `yaml:"receive_wait_time_seconds" envconfig:"SQS_WAIT_TIME_SECONDS"`
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
	// visibility timeout should default to nil to use the overall visibility timeout for the queue
	VisibilityTimeout *int `yaml:"receive_visibility_timeout" envconfig:"SQS_VISIBILITY_TIMEOUT"`
}

// RedisConfig ...
type RedisConfig struct {
	// Maximum number of idle connections in the pool.
	MaxIdle int `yaml:"max_idle" envconfig:"REDIS_MAX_IDLE"`

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int `yaml:"max_active" envconfig:"REDIS_MAX_ACTIVE"`

	// Close connections after remaining idle for this duration in seconds. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout int `yaml:"max_idle_timeout" envconfig:"REDIS_IDLE_TIMEOUT"`

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool `yaml:"wait" envconfig:"REDIS_WAIT"`

	// ReadTimeout specifies the timeout in seconds for reading a single command reply.
	ReadTimeout int `yaml:"read_timeout" envconfig:"REDIS_READ_TIMEOUT"`

	// WriteTimeout specifies the timeout in seconds for writing a single command.
	WriteTimeout int `yaml:"write_timeout" envconfig:"REDIS_WRITE_TIMEOUT"`

	// ConnectTimeout specifies the timeout in seconds for connecting to the Redis server when
	// no DialNetDial option is specified.
	ConnectTimeout int `yaml:"connect_timeout" envconfig:"REDIS_CONNECT_TIMEOUT"`

	// DelayedTasksPollPeriod specifies the period in milliseconds when polling redis for delayed tasks
	DelayedTasksPollPeriod int `yaml:"delayed_tasks_poll_period" envconfig:"REDIS_DELAYED_TASKS_POLL_PERIOD"`
}

// GCPPubSubConfig wraps GCP PubSub related configuration
type GCPPubSubConfig struct {
	Client       *pubsub.Client
	MaxExtension time.Duration
}

// MongoDBConfig ...
type MongoDBConfig struct {
	Client   *mongo.Client
	Database string
}

// Decode from yaml to map (any field whose type or pointer-to-type implements
// envconfig.Decoder can control its own deserialization)
func (args *QueueBindingArgs) Decode(value string) error {
	pairs := strings.Split(value, ",")
	mp := make(map[string]interface{}, len(pairs))
	for _, pair := range pairs {
		kvpair := strings.Split(pair, ":")
		if len(kvpair) != 2 {
			return fmt.Errorf("invalid map item: %q", pair)
		}
		mp[kvpair[0]] = kvpair[1]
	}
	*args = QueueBindingArgs(mp)
	return nil
}
