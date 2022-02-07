[1]: https://raw.githubusercontent.com/Michael-LiK/assets/master/machinery/example_worker.png
[2]: https://raw.githubusercontent.com/Michael-LiK/assets/master/machinery/example_worker_receives_tasks.png
[3]: http://patreon_public_assets.s3.amazonaws.com/sized/becomeAPatronBanner.png

## Machinery

Machinery 是一个基于分布式消息传递的异步任务队列框架.

[![Travis Status for Michael-LiK/machinery](https://travis-ci.org/Michael-LiK/machinery.svg?branch=master&label=linux+build)](https://travis-ci.org/Michael-LiK/machinery)
[![godoc for Michael-LiK/machinery](https://godoc.org/github.com/nathany/looper?status.svg)](http://godoc.org/github.com/Michael-LiK/machinery/v1)
[![codecov for Michael-LiK/machinery](https://codecov.io/gh/Michael-LiK/machinery/branch/master/graph/badge.svg)](https://codecov.io/gh/Michael-LiK/machinery)

[![Go Report Card](https://goreportcard.com/badge/github.com/Michael-LiK/machinery)](https://goreportcard.com/report/github.com/Michael-LiK/machinery)
[![GolangCI](https://golangci.com/badges/github.com/Michael-LiK/machinery.svg)](https://golangci.com)
[![OpenTracing Badge](https://img.shields.io/badge/OpenTracing-enabled-blue.svg)](http://opentracing.io)

[![Sourcegraph for Michael-LiK/machinery](https://sourcegraph.com/github.com/Michael-LiK/machinery/-/badge.svg)](https://sourcegraph.com/github.com/Michael-LiK/machinery?badge)
[![Donate Bitcoin](https://img.shields.io/badge/donate-bitcoin-orange.svg)](https://Michael-LiK.github.io/donate/)

---

* [V2 Experiment](#v2-experiment)
* [First Steps](#快速开始)
* [Configuration](#configuration)
  * [Lock](#lock)
  * [Broker](#broker)
  * [DefaultQueue](#defaultqueue)
  * [ResultBackend](#resultbackend)
  * [ResultsExpireIn](#resultsexpirein)
  * [AMQP](#amqp-2)
  * [DynamoDB](#dynamodb)
  * [Redis](#redis-2)
  * [GCPPubSub](#gcppubsub)
* [Custom Logger](#custom-logger)
* [Server](#server)
* [Workers](#workers)
* [Tasks](#tasks)
  * [Registering Tasks](#registering-tasks)
  * [Signatures](#signatures)
  * [Supported Types](#supported-types)
  * [Sending Tasks](#sending-tasks)
  * [Delayed Tasks](#delayed-tasks)
  * [Retry Tasks](#retry-tasks)
  * [Get Pending Tasks](#get-pending-tasks)
  * [Keeping Results](#keeping-results)
* [Workflows](#workflows)
  * [Groups](#groups)
  * [Chords](#chords)
  * [Chains](#chains)
* [Periodic Tasks & Workflows](#periodic-tasks--workflows)
  * [Periodic Tasks](#periodic-tasks)
  * [Periodic Groups](#periodic-groups)
  * [Periodic Chains](#periodic-chains)
  * [Periodic Chords](#periodic-chords)
* [Development](#development)
  * [Requirements](#requirements)
  * [Dependencies](#dependencies)
  * [Testing](#testing)

### V2 实验版本

需要注意的是v2版本目前正在研发中，所以在正式版发布前，v2可能会有较大的改变。

你可以使用v2版本来避免导入那些你没有使用的所有依赖。

Instead of factory, you will need to inject broker and backend objects to the server constructor:

```go
import (
  "github.com/Michael-LiK/machinery/v2"
  backendsiface "github.com/Michael-LiK/machinery/v2/backends/iface"
  brokersiface "github.com/Michael-LiK/machinery/v2/brokers/iface"
  locksiface "github.com/Michael-LiK/machinery/v2/locks/iface"
)

var broker brokersiface.Broker
var backend backendsiface.Backend
var lock locksiface.Lock
server := machinery.NewServer(cnf, broker, backend, lock)
// server.NewWorker("machinery", 10)
```

### 快速开始

将Machinery依赖添加到你的 $GOPATH/src:

```sh
go get github.com/Michael-LiK/machinery/v1
```

或者使用实验版本 v2 release:

```sh
go get github.com/Michael-LiK/machinery/v2
```

首先你需要定义一些任务. 可以浏览示例 `example/tasks/tasks.go` 这里有一些简单的例子.

然后, 你讲通过一下的一些命令去发布一个工作进程 (v2 建议不要导入所有的 brokers / backends, 只导入你真正需要的即可):

```sh
go run example/amqp/main.go worker
go run example/redis/main.go worker

go run example/amqp/main.go worker
go run example/redis/main.go worker
```

你也可以使用v2中的例子.

```sh
cd v2/
go run example/amqp/main.go worker
go run example/redigo/main.go worker // Redis with redigo driver
go run example/go-redis/main.go worker // Redis with Go Redis driver

go run example/amqp/main.go worker
go run example/redis/main.go worker
```

![Example worker][1]

最后, 只要你一个进程在运行，通过下面这些命令发送任务，并且等在任务去消费即可。 (建议使用v2 ，因为它不会导入所有代理/后端的依赖项，而只导入您实际需要的依赖项):

```sh
go run example/v2/amqp/main.go send
go run example/v2/redigo/main.go send // Redis with redigo driver
go run example/v2/go-redis/main.go send // Redis with Go Redis driver

go run example/v1/amqp/main.go send
go run example/v1/redis/main.go send
```

你将可以看到任务正在被进程异步的处理了:

![Example worker receives tasks][2]

### 配置

 [config](/v1/config/config.go) 配置有多种方便的加载方式，可以从环境变量或者YAML文件中去加载配置:

```go
cnf, err := config.NewFromEnvironment()
```

Or load from YAML file:

```go
cnf, err := config.NewFromYaml("config.yml", true)
```

第二个布尔值参数代表是否进行实时的配置重载，如果为true，系统将每个10秒进行一次配置重载，如果为false则不进行实时配置重载。

Machinery 配置通过一个名为 `Config` 的结构体进行封装，并作为依赖项注入到需要它的对象中。

#### Lock

##### Redis

使用redis链接需要按照如下几种格式:

```
redis://[password@]host[port][/db_num]
```

例如:

1. `redis://localhost:6379`, 或者使用密码 `redis://password@localhost:6379`

#### Broker

消息 broker，现在支持多种 brokers 他们分别是:

##### AMQP

使用AMQP链接需要按照如下几种格式:

```
amqp://[username:password@]@host[:port]
```

例如:

1. `amqp://guest:guest@localhost:5672`

AMQP 也支持多个 brokers 链接.你需要具体填写 URL separator 在`MultipleBrokerSeparator` 字段.

##### Redis

使用redis链接需要按照如下几种格式:

```
redis://[password@]host[port][/db_num]
redis+socket://[password@]/path/to/file.sock[:/db_num]
```

例如:

1. `redis://localhost:6379`, 或者使用密码 `redis://password@localhost:6379`
2. `redis+socket://password@/path/to/file.sock:/0`

##### AWS SQS

使用 AWS SQS 链接需要按照如下格式:

```
https://sqs.us-east-2.amazonaws.com/123456789012
```

查看 [AWS SQS 文档](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html) 可以了解更多信息。
此外还需要配置`AWS_REGION`，否则一个error将被抛出。

手动配置 SQS Client:

```go
var sqsClient = sqs.New(session.Must(session.NewSession(&aws.Config{
  Region:         aws.String("YOUR_AWS_REGION"),
  Credentials:    credentials.NewStaticCredentials("YOUR_AWS_ACCESS_KEY", "YOUR_AWS_ACCESS_SECRET", ""),
  HTTPClient:     &http.Client{
    Timeout: time.Second * 120,
  },
})))
var visibilityTimeout = 20
var cnf = &config.Config{
  Broker:          "YOUR_SQS_URL"
  DefaultQueue:    "machinery_tasks",
  ResultBackend:   "YOUR_BACKEND_URL",
  SQS: &config.SQSConfig{
    Client: sqsClient,
    // if VisibilityTimeout is nil default to the overall visibility timeout setting for the queue
    // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
    VisibilityTimeout: &visibilityTimeout,
    WaitTimeSeconds: 30,
  },
}
```

##### GCP Pub/Sub

使用 GCP Pub/Sub URL 在如下格式:

```
gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME
```

手动配置 Pub/Sub Client:

```go
pubsubClient, err := pubsub.NewClient(
    context.Background(),
    "YOUR_GCP_PROJECT_ID",
    option.WithServiceAccountFile("YOUR_GCP_SERVICE_ACCOUNT_FILE"),
)

cnf := &config.Config{
  Broker:          "gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME"
  DefaultQueue:    "YOUR_PUBSUB_TOPIC_NAME",
  ResultBackend:   "YOUR_BACKEND_URL",
  GCPPubSub: config.GCPPubSubConfig{
    Client: pubsubClient,
  },
}
```

#### 默认队列（DefaultQueue）

默认队列名称, 例如: `machinery_tasks`.

#### 结果存放（ResultBackend）

Result backend 被用来保存任务的状态和结果.

现在支持的结果存放有如下这些:

##### Redis

使用Redis URL 在如下格式:

```
redis://[password@]host[port][/db_num]
redis+socket://[password@]/path/to/file.sock[:/db_num]
```

例如:

1. `redis://localhost:6379`, 或者使用密码 `redis://password@localhost:6379`
2. `redis+socket://password@/path/to/file.sock:/0`
3. cluster `redis://host1:port1,host2:port2,host3:port3`
4. cluster with password `redis://pass@host1:port1,host2:port2,host3:port3`

##### Memcache

使用 Memcache URL 在如下格式:

```
memcache://host1[:port1][,host2[:port2],...[,hostN[:portN]]]
```

例如:

1. `memcache://localhost:11211`一个简单实例，或者：
2. `memcache://10.0.0.1:11211,10.0.0.2:11211` 一个集群

##### AMQP

使用 AMQP URL 采用以下格式:

```
amqp://[username:password@]@host[:port]
```

例如:

1. `amqp://guest:guest@localhost:5672`

> 请注意 AMQP 不被推荐作为结果存储使用. 详情见 [Keeping Results](https://github.com/Michael-LiK/machinery#keeping-results)

##### MongoDB

使用 Mongodb URL 采用以下格式:

```
mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
```

例如:

1. `mongodb://localhost:27017/taskresults`

查看 [MongoDB docs](https://docs.mongodb.org/manual/reference/connection-string/) 获得更新信息.


#### 结果有效期

任务结果的存储过期时间默认是 `3600`秒 (1 小时).

#### AMQP

RabbitMQ 相关配置. 如果你使用其他 broker/backend 这部分没有参考必要.

* `Exchange`: exchange 名称, 例如： `machinery_exchange`
* `ExchangeType`: exchange 类型, 例如： `direct`
* `QueueBindingArguments`: 绑定到AMQP队列时使用的附加参数的可选映射
* `BindingKey`:  使用此密钥将队列绑定到exchange，例如： `machinery_task`
* `PrefetchCount`: 要预取的任务数 (如果有长时间运行的任务，则设置为“1”)

#### DynamoDB

DynamoDB 相关设置. 如果你是用其他 backend 这部分没有参考必要.
* `TaskStatesTable`: 自定义存储任务状态的表名称。默认使用 `task_states`, 并确保首先在AWS管理员中创建此表, 使用 `TaskUUID` 作为表的主键.
* `GroupMetasTable`: 自定义存储组meta信息的表名称。默认使用 `group_metas`, 并确保首先在AWS管理员中创建此表, 使用 `GroupUUID` 作为表的主键.

* 例如:

```
dynamodb:
  task_states_table: 'task_states'
  group_metas_table: 'group_metas'
```
如果找不到这些表，将抛出致命错误。

如果你希望记录会过期, 你可以在AWS admin中给这些表配置`TTL`字段. 这个 `TTL` 字段是基于服务配置中的 `ResultsExpireIn` 值. 查看 https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html获取更多信息。

#### Redis

Redis 相关配置. 如果你是用其他 backend 这部分没有参考必要。

See: [config](/v1/config/config.go) (TODO)

#### GCPPubSub

GCPPubSub 相关配置. 如果你是用其他 backend 这部分没有参考必要.

See: [config](/v1/config/config.go) (TODO)

### Custom Logger

你可以定义一个用户级别日期通过实现一下的接口:

```go
type Interface interface {
  Print(...interface{})
  Printf(string, ...interface{})
  Println(...interface{})

  Fatal(...interface{})
  Fatalf(string, ...interface{})
  Fatalln(...interface{})

  Panic(...interface{})
  Panicf(string, ...interface{})
  Panicln(...interface{})
}
```

然后就可以设置日志在你的安装代码中使用`Set` 方式，通过`github.com/Michael-LiK/machinery/v1/log` 这个包:

```go
log.Set(myCustomLogger)
```

### Server

一个 Machinery library在被使用之前必须被实例化.通过以下方式创建一个 `Server` 实例. `Server` 是一个存储项目配置和任务注册的基础对象。例如:

```go
import (
  "github.com/Michael-LiK/machinery/v1/config"
  "github.com/Michael-LiK/machinery/v1"
)

var cnf = &config.Config{
  Broker:        "amqp://guest:guest@localhost:5672/",
  DefaultQueue:  "machinery_tasks",
  ResultBackend: "amqp://guest:guest@localhost:5672/",
  AMQP: &config.AMQPConfig{
    Exchange:     "machinery_exchange",
    ExchangeType: "direct",
    BindingKey:   "machinery_task",
  },
}

server, err := machinery.NewServer(cnf)
if err != nil {
  // do something with the error
}
```

### Workers

为了去消费任务, 你需要有一个或者多个 Workers运行. 你想运行worker仅需一个具有已注册任务的“服务器”实例。例如：

```go 
worker := server.NewWorker("worker_name", 10)
err := worker.Launch()
if err != nil {
  // do something with the error
}
```

每个 worker 都将只消费已注册的任务. 对于队列上的每个任务 Worker.Process() 方法都将运行在一个goroutine中. 使用 `server.NewWorker`方法的第二个参数去限制并发运行的Worker.Process()的调用数量
(单个 worker). 例如: 设置1将使任务串行执行，而0使并发执行的任务数不受限制（默认）。

### Tasks

任务是Machinery应用的组成部分. 一个任务是一个函数， 一个被用户接收到消息来触发的函数。

每一个任务都需要返回一个error作为最后一个返回值， In addition to error tasks can now return any number of arguments.

有效任务的示例：

```go
func Add(args ...int64) (int64, error) {
  sum := int64(0)
  for _, arg := range args {
    sum += arg
  }
  return sum, nil
}

func Multiply(args ...int64) (int64, error) {
  sum := int64(1)
  for _, arg := range args {
    sum *= arg
  }
  return sum, nil
}

// You can use context.Context as first argument to tasks, useful for open tracing
func TaskWithContext(ctx context.Context, arg Arg) error {
  // ... use ctx ...
  return nil
}

// Tasks need to return at least error as a minimal requirement
func DummyTask(arg string) error {
  return errors.New(arg)
}

// You can also return multiple results from the task
func DummyTask2(arg1, arg2 string) (string, string, error) {
  return arg1, arg2, nil
}
```

#### 注册任务

在你的workers消费任务前, 你需要在server中注册它. 下面是为任务分配唯一的名称:

```go
server.RegisterTasks(map[string]interface{}{
  "add":      Add,
  "multiply": Multiply,
})
```

任务也可以被一个一个注册:

```go
server.RegisterTask("add", Add)
server.RegisterTask("multiply", Multiply)
```

简单的推送, 当一个工作进程接收到如下消息:

```json
{
  "UUID": "48760a1a-8576-4536-973b-da09048c2ac5",
  "Name": "add",
  "RoutingKey": "",
  "ETA": null,
  "GroupUUID": "",
  "GroupTaskCount": 0,
  "Args": [
    {
      "Type": "int64",
      "Value": 1,
    },
    {
      "Type": "int64",
      "Value": 1,
    }
  ],
  "Immutable": false,
  "RetryCount": 0,
  "RetryTimeout": 0,
  "OnSuccess": null,
  "OnError": null,
  "ChordCallback": null
}
```


它将调用 Add(1, 1)。每个任务也应该返回一个错误，以便我们可以处理失败。

理想情况下，任务应该是幂等的，这意味着当使用相同参数多次调用任务时不会出现意外后果。

#### Signatures

签名包装了任务的调用参数、执行选项（例如不变性）和成功/错误回调，以便它可以通过线路发送给工作人员。任务签名实现了一个简单的接口：

```go
// Arg represents a single argument passed to invocation fo a task
type Arg struct {
  Type  string
  Value interface{}
}

// Headers represents the headers which should be used to direct the task
type Headers map[string]interface{}

// Signature represents a single task invocation
type Signature struct {
  UUID           string
  Name           string
  RoutingKey     string
  ETA            *time.Time
  GroupUUID      string
  GroupTaskCount int
  Args           []Arg
  Headers        Headers
  Immutable      bool
  RetryCount     int
  RetryTimeout   int
  OnSuccess      []*Signature
  OnError        []*Signature
  ChordCallback  *Signature
}
```

`UUID` 一个任务的唯一ID。您可以自己设置，也可以自动生成。

`Name` 任务的唯一名称,是针对服务器实例注册的唯一任务名称。

`RoutingKey` 用于将任务路由到正确的队列。如果将其保留为空，默认行为将是将其设置为直接exchange类型的默认队列绑定键，以及其他exchange类型的默认队列名称。


`ETA` 是一个用于设置任务延迟的时间戳字段。如果他为空，则这个任务将会被立即推送到woker中进行消费。 如果它被设置，这个任务将会被延迟到 ETA 时间戳.

`GroupUUID`, `GroupTaskCount` 被用于创建给任务组。

`Args` 是由worker执行任务时将传递给该任务的参数列表。

`Headers` 将任务发布到AMQP队列时将使用的headers列表。

`Immutable` 是一个标志，用于定义是否可以修改已执行任务的结果。 这对于 `OnSuccess` 回调很重要。不可变任务不会将其结果传递给它的成功回调，而可变任务会将其结果添加到发送给回调任务的参数中。长话短说，如果要将链中第一个任务的结果传递给第二个任务，请将 Immutable 设置为 false。

`RetryCount` 指定应重试失败任务的次数（默认为 0）。重试尝试将按时间间隔，在每次失败后，下一次重试时间将更往后。

`RetryTimeout` 指定在将任务重新发送到队列以进行重试之前等待多长时间。默认行为是使用斐波那契数列来增加每次失败的重试尝试后的超时时间。

`OnSuccess`定义任务成功执行后将调用的任务。它是任务声明结构的一部分。

`OnError` 定义任务执行失败后将调用的任务。传递给错误回调的第一个参数将是从失败的任务返回的错误字符串。

`ChordCallback` 用于创建对一组任务的回调。

#### Supported Types

Machinery 在发送任务到broker前将任务编码为json.任务结果也将以JSON字符串格式被存储在backend中。因此，只能支持具有原生 JSON 表示的类型。目前支持的类型是

* `bool`
* `int`
* `int8`
* `int16`
* `int32`
* `int64`
* `uint`
* `uint8`
* `uint16`
* `uint32`
* `uint64`
* `float32`
* `float64`
* `string`
* `[]bool`
* `[]int`
* `[]int8`
* `[]int16`
* `[]int32`
* `[]int64`
* `[]uint`
* `[]uint8`
* `[]uint16`
* `[]uint32`
* `[]uint64`
* `[]float32`
* `[]float64`
* `[]string`

#### 发送任务

任务能够通过`Signature`的一个实例被调用到另一个`Server`实例。例如:

```go
import (
  "github.com/Michael-LiK/machinery/v1/tasks"
)

signature := &tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 1,
    },
    {
      Type:  "int64",
      Value: 1,
    },
  },
}

asyncResult, err := server.SendTask(signature)
if err != nil {
  // failed to send the task
  // do something with the error
}
```

#### Delayed Tasks

你可以延期一个任务通过在任务的声明中设置 `ETA` 时间戳字段。

```go
// Delay the task by 5 seconds
eta := time.Now().UTC().Add(time.Second * 5)
signature.ETA = &eta
```

#### Retry Tasks

你可以设置一个重试次数在任务被声明为失败前. 斐波那契序列将用于在一段时间内分隔重试请求。(详情见 `RetryTimeout`)

```go
// If the task fails, retry it up to 3 times
signature.RetryCount = 3
```

或者, 你可以从任务重返回 `tasks.ErrRetryTaskLater`并指定应重试任务的持续时间，例如：

```go
return tasks.NewErrRetryTaskLater("some error", 4 * time.Hour)
```

#### Get Pending Tasks

可以检查当前在队列中等待workers消费的任务，例如：

```go
server.GetBroker().GetPendingTasks("some_queue")
```

>目前仅支持 Redis broker .

#### Keeping Results

如果您配置结果backend，任务状态和结果将被持久化。可能的状态：

```go
const (
	// StatePending - initial state of a task
	StatePending = "PENDING"
	// StateReceived - when task is received by a worker
	StateReceived = "RECEIVED"
	// StateStarted - when the worker starts processing the task
	StateStarted = "STARTED"
	// StateRetry - when failed task has been scheduled for retry
	StateRetry = "RETRY"
	// StateSuccess - when the task is processed successfully
	StateSuccess = "SUCCESS"
	// StateFailure - when processing of the task fails
	StateFailure = "FAILURE"
)
```

> 当使用 AMQP 作为结果backend时，任务状态将保存在每个任务的单独队列中。尽管 RabbitMQ 可以扩展到数千个队列，但强烈建议在您期望运行大量并行任务时使用更合适的结果后端（例如 Memcache）。
```go
// TaskResult represents an actual return value of a processed task
type TaskResult struct {
  Type  string      `bson:"type"`
  Value interface{} `bson:"value"`
}

// TaskState represents a state of a task
type TaskState struct {
  TaskUUID  string        `bson:"_id"`
  State     string        `bson:"state"`
  Results   []*TaskResult `bson:"results"`
  Error     string        `bson:"error"`
}

// GroupMeta stores useful metadata about tasks within the same group
// E.g. UUIDs of all tasks which are used in order to check if all tasks
// completed successfully or not and thus whether to trigger chord callback
type GroupMeta struct {
  GroupUUID      string   `bson:"_id"`
  TaskUUIDs      []string `bson:"task_uuids"`
  ChordTriggered bool     `bson:"chord_triggered"`
  Lock           bool     `bson:"lock"`
}
```

`TaskResult` 表示已处理任务的返回值切片。

`TaskState` 每次任务状态发生变化时，TaskState struct 都会被序列化并存储。

`GroupMeta` 存储有关同一组内任务的有用元数据。例如。所有任务的 UUID 用于检查所有任务是否成功完成，从而是否触发和弦回调。

`AsyncResult` AsyncResult对象允许您检查任务的状态：

```go
taskState := asyncResult.GetState()
fmt.Printf("Current state of %v task is:\n", taskState.TaskUUID)
fmt.Println(taskState.State)
```

有几种方便的方法可以检查任务状态：

```go
asyncResult.GetState().IsCompleted()
asyncResult.GetState().IsSuccess()
asyncResult.GetState().IsFailure()
```

你也可以做一个同步阻塞调用来等待一个任务结果：

```go
results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
if err != nil {
  // getting result of a task failed
  // do something with the error
}
for _, result := range results {
  fmt.Println(result.Interface())
}
```

#### Error Handling

当任务返回错误时，如果它是可重试的，默认行为是首先尝试重试任务，否则记录错误，然后最终调用任何错误回调。

要对此进行自定义，您可以在工作线程上设置自定义错误处理程序，该处理程序不仅可以在重试失败和错误回调触发后进行记录，还可以执行更多操作：

```go
worker.SetErrorHandler(func (err error) {
  customHandler(err)
})
```

### 工作流

运行一个简单的异步认识是很容易，但是通常你希望能够设计一个任务编排的工作流，这里有几种有用的方式去帮助你设计工作流。

#### Groups

`Group` 是一组相互独立并行执行的任务。例如。：

```go
import (
  "github.com/Michael-LiK/machinery/v1/tasks"
  "github.com/Michael-LiK/machinery/v1"
)

signature1 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 1,
    },
    {
      Type:  "int64",
      Value: 1,
    },
  },
}

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

group, _ := tasks.NewGroup(&signature1, &signature2)
asyncResults, err := server.SendGroup(group, 0) //The second parameter specifies the number of concurrent sending tasks. 0 means unlimited.
if err != nil {
  // failed to send the group
  // do something with the error
}
```

`SendGroup` 返回一个 `AsyncResult` 的对象。因此，您可以执行阻塞调用并等待组任务的结果:

```go
for _, asyncResult := range asyncResults {
  results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
  if err != nil {
    // getting result of a task failed
    // do something with the error
  }
  for _, result := range results {
    fmt.Println(result.Interface())
  }
}
```

#### Chords

`Chord` 允许您定义在组中的所有任务完成处理后执行的回调，例如：

```go
import (
  "github.com/Michael-LiK/machinery/v1/tasks"
  "github.com/Michael-LiK/machinery/v1"
)

signature1 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 1,
    },
    {
      Type:  "int64",
      Value: 1,
    },
  },
}

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

signature3 := tasks.Signature{
  Name: "multiply",
}

group := tasks.NewGroup(&signature1, &signature2)
chord, _ := tasks.NewChord(group, &signature3)
chordAsyncResult, err := server.SendChord(chord, 0) //The second parameter specifies the number of concurrent sending tasks. 0 means unlimited.
if err != nil {
  // failed to send the chord
  // do something with the error
}
```

上面的示例并行执行 task1 和 task2，聚合它们的结果并将它们传递给 task3。因此，最终会展示的是：

```
multiply(add(1, 1), add(5, 5))
```

更明确的样子:

```
(1 + 1) * (5 + 5) = 2 * 10 = 20
```

`SendChord` 返回遵循 AsyncResult 接口的 `ChordAsyncResult`。所以你可以做一个阻塞调用并等待回调的结果：

```go
results, err := chordAsyncResult.Get(time.Duration(time.Millisecond * 5))
if err != nil {
  // getting result of a chord failed
  // do something with the error
}
for _, result := range results {
  fmt.Println(result.Interface())
}
```

#### Chains

`Chain` 只是一组将逐个执行的任务，每个成功的任务都会触发链中的下一个任务。例如：

```go
import (
  "github.com/Michael-LiK/machinery/v1/tasks"
  "github.com/Michael-LiK/machinery/v1"
)

signature1 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 1,
    },
    {
      Type:  "int64",
      Value: 1,
    },
  },
}

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

signature3 := tasks.Signature{
  Name: "multiply",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 4,
    },
  },
}

chain, _ := tasks.NewChain(&signature1, &signature2, &signature3)
chainAsyncResult, err := server.SendChain(chain)
if err != nil {
  // failed to send the chain
  // do something with the error
}
```

上面的例子执行task1，然后是task2，然后是task3。当任务成功完成时，结果将附加到链中下一个任务的参数列表的末尾。因此，最终会展现的是：

```
multiply(4, add(5, 5, add(1, 1)))
```

More explicitly:

```
  4 * (5 + 5 + (1 + 1))   # task1: add(1, 1)        returns 2
= 4 * (5 + 5 + 2)         # task2: add(5, 5, 2)     returns 12
= 4 * (12)                # task3: multiply(4, 12)  returns 48
= 48
```

`SendChain` 返回遵循 AsyncResult 接口的 `ChainAsyncResult`。所以你可以做一个阻塞调用并等待整个链的结果：

```go
results, err := chainAsyncResult.Get(time.Duration(time.Millisecond * 5))
if err != nil {
  // getting result of a chain failed
  // do something with the error
}
for _, result := range results {
  fmt.Println(result.Interface())
}
```

### Periodic Tasks & Workflows

Machinery 现在支持安排定期任务和工作流。请参见下面的示例。

#### Periodic Tasks

```go
import (
  "github.com/Michael-LiK/machinery/v1/tasks"
)

signature := &tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 1,
    },
    {
      Type:  "int64",
      Value: 1,
    },
  },
}
err := server.RegisterPeriodicTask("0 6 * * ?", "periodic-task", signature)
if err != nil {
  // failed to register periodic task
}
```

#### Periodic Groups

```go
import (
  "github.com/Michael-LiK/machinery/v1/tasks"
  "github.com/Michael-LiK/machinery/v1"
)

signature1 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 1,
    },
    {
      Type:  "int64",
      Value: 1,
    },
  },
}

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

group, _ := tasks.NewGroup(&signature1, &signature2)
err := server.RegisterPeriodicGroup("0 6 * * ?", "periodic-group", group)
if err != nil {
  // failed to register periodic group
}
```

#### Periodic Chains

```go
import (
  "github.com/Michael-LiK/machinery/v1/tasks"
  "github.com/Michael-LiK/machinery/v1"
)

signature1 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 1,
    },
    {
      Type:  "int64",
      Value: 1,
    },
  },
}

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

signature3 := tasks.Signature{
  Name: "multiply",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 4,
    },
  },
}

chain, _ := tasks.NewChain(&signature1, &signature2, &signature3)
err := server.RegisterPeriodicChain("0 6 * * ?", "periodic-chain", chain)
if err != nil {
  // failed to register periodic chain
}
```

#### Chord

```go
import (
  "github.com/Michael-LiK/machinery/v1/tasks"
  "github.com/Michael-LiK/machinery/v1"
)

signature1 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 1,
    },
    {
      Type:  "int64",
      Value: 1,
    },
  },
}

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

signature3 := tasks.Signature{
  Name: "multiply",
}

group := tasks.NewGroup(&signature1, &signature2)
chord, _ := tasks.NewChord(group, &signature3)
err := server.RegisterPeriodicChord("0 6 * * ?", "periodic-chord", chord)
if err != nil {
  // failed to register periodic chord
}
```

### 开发相关

#### 环境要求

* Go
* RabbitMQ (可选项)
* Redis
* Memcached (可选项)
* MongoDB (可选项)

On OS X systems, 你可以使用homebrew安装环境 [Homebrew](http://brew.sh/):

```sh
brew install go
brew install rabbitmq
brew install redis
brew install memcached
brew install mongodb
```

或者可选地使用相应的 [Docker](http://docker.io/) 容器:

```
docker run -d -p 5672:5672 rabbitmq
docker run -d -p 6379:6379 redis
docker run -d -p 11211:11211 memcached
docker run -d -p 27017:27017 mongo
docker run -d -p 6831:6831/udp -p 16686:16686 jaegertracing/all-in-one:latest
```

#### 依赖

从 Go 1.11起, 一种新的依赖管理系统被通过（go module） [modules](https://github.com/golang/go/wiki/Modules).

这是 Go 的一个小问题，不过限制依赖管理已经被解决。以前 Go 官方推荐使用 [dep 工具](https://github.com/golang/dep)，但现在已经放弃，转而支持模块。

#### 测试

运行测试的最简单（和平台无关）方法是通过 `docker-compose`:

```sh
make ci
```

运行 docker-compose 命令：

```sh
(docker-compose -f docker-compose.test.yml -p machinery_ci up --build -d) && (docker logs -f machinery_sut &) && (docker wait machinery_sut)
```

另一种方法是在您的机器上设置开发环境。

为了启用集成测试，您需要安装所有必需的服务（RabbitMQ、Redis、Memcache、MongoDB）并导出这些环境变量：

```sh
export AMQP_URL=amqp://guest:guest@localhost:5672/
export REDIS_URL=localhost:6379
export MEMCACHE_URL=localhost:11211
export MONGODB_URL=localhost:27017
```

要针对 SQS 实例运行集成测试，您需要在 SQS 中创建一个“test_queue”并导出这些环境变量：

```sh
export SQS_URL=https://YOUR_SQS_URL
export AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=YOUR_AWS_DEFAULT_REGION
```

然后运行:

```sh
make test
```

如果未导出环境变量，`make test` 将仅运行单元测试。
