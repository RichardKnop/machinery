module github.com/RichardKnop/machinery

require (
	cloud.google.com/go v0.72.0 // indirect
	cloud.google.com/go/pubsub v1.8.3
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae
	github.com/aws/aws-sdk-go v1.35.28
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/go-redis/redis/v8 v8.3.3
	github.com/go-redsync/redsync/v4 v4.0.3
	github.com/golang/snappy v0.0.2 // indirect
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/go-cmp v0.5.3 // indirect
	github.com/google/uuid v1.1.2
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/klauspost/compress v1.11.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/streadway/amqp v1.0.0
	github.com/stretchr/testify v1.6.1
	github.com/urfave/cli v1.22.5
	github.com/xdg/stringprep v1.0.0 // indirect
	go.mongodb.org/mongo-driver v1.4.3
	golang.org/x/crypto v0.0.0-20201112155050-0c6587e931a9 // indirect
	golang.org/x/mod v0.3.1-0.20200828183125-ce943fd02449 // indirect
	golang.org/x/net v0.0.0-20201110031124-69a78807bb2b // indirect
	golang.org/x/oauth2 v0.0.0-20201109201403-9fd604954f58 // indirect
	golang.org/x/sys v0.0.0-20201113233024-12cec1faf1ba // indirect
	golang.org/x/tools v0.0.0-20201113202037-1643af1435f3 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20201113130914-ce600e9a6f9e // indirect
	gopkg.in/yaml.v2 v2.3.0
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

replace github.com/go-redsync/redsync/v4 => github.com/darful/redsync/v4 v4.0.4-0.20201117201638-f97e91701f52

go 1.13
