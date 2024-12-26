module github.com/RichardKnop/machinery/v2

go 1.15

require (
	cloud.google.com/go/pubsub v1.10.0
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae
	github.com/aws/aws-sdk-go v1.37.16
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/go-redsync/redsync/v4 v4.8.1
	github.com/gomodule/redigo v1.9.2
	github.com/google/uuid v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/rabbitmq/amqp091-go v1.9.0
	github.com/redis/go-redis/v9 v9.0.5
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.8.4
	github.com/urfave/cli v1.22.5
	go.mongodb.org/mongo-driver v1.17.0
	gopkg.in/yaml.v2 v2.4.0
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
