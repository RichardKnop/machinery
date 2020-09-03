module github.com/RichardKnop/machinery

require (
	cloud.google.com/go v0.61.0 // indirect
	cloud.google.com/go/pubsub v1.5.0
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae
	github.com/RichardKnop/redsync v1.2.0
	github.com/aws/aws-sdk-go v1.33.6
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/go-redis/redis v6.15.8+incompatible
	github.com/go-redis/redis/v8 v8.0.0-beta.6
	github.com/gomodule/redigo v1.8.2
	github.com/google/uuid v1.1.1
	github.com/juju/fslock v0.0.0-20160525022230-4d5c94c67b4b
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/kisielk/errcheck v1.2.0 // indirect
	github.com/klauspost/compress v1.10.10 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/streadway/amqp v1.0.0
	github.com/stretchr/testify v1.6.1
	github.com/stvp/tempredis v0.0.0-20181119212430-b82af8480203 // indirect
	github.com/urfave/cli v1.22.4
	github.com/xdg/stringprep v1.0.0 // indirect
	go.mongodb.org/mongo-driver v1.3.5
	go.opentelemetry.io/otel v0.8.0 // indirect
	golang.org/x/crypto v0.0.0-20200709230013-948cd5f35899 // indirect
	golang.org/x/sys v0.0.0-20200625212154-ddb9806d33ae // indirect
	golang.org/x/tools v0.0.0-20200715235423-130c9f19d3fe // indirect
	google.golang.org/genproto v0.0.0-20200715011427-11fb19a81f2c // indirect
	gopkg.in/fsnotify.v1 v1.4.7
	gopkg.in/yaml.v2 v2.3.0
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

go 1.13
