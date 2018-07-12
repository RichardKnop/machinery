package machinery

import (
	"errors"
	"fmt"
	neturl "net/url"
	"strconv"
	"strings"

	"github.com/RichardKnop/machinery/v1/config"

	amqpbroker "github.com/RichardKnop/machinery/v1/brokers/amqp"
	eagerbroker "github.com/RichardKnop/machinery/v1/brokers/eager"
	brokeriface "github.com/RichardKnop/machinery/v1/brokers/iface"
	redisbroker "github.com/RichardKnop/machinery/v1/brokers/redis"
	sqsbroker "github.com/RichardKnop/machinery/v1/brokers/sqs"
	cmqbroker "github.com/RichardKnop/machinery/v1/brokers/cmq"

	amqpbackend "github.com/RichardKnop/machinery/v1/backends/amqp"
	dynamobackend "github.com/RichardKnop/machinery/v1/backends/dynamodb"
	eagerbackend "github.com/RichardKnop/machinery/v1/backends/eager"
	backendiface "github.com/RichardKnop/machinery/v1/backends/iface"
	memcachebackend "github.com/RichardKnop/machinery/v1/backends/memcache"
	mongobackend "github.com/RichardKnop/machinery/v1/backends/mongo"
	redisbackend "github.com/RichardKnop/machinery/v1/backends/redis"
	"github.com/baocaixiong/cmq-golang-sdk"
)

// BrokerFactory creates a new object of iface.Broker
// Currently only AMQP/S broker is supported
func BrokerFactory(cnf *config.Config) (brokeriface.Broker, error) {
	if strings.HasPrefix(cnf.Broker, "amqp://") {
		return amqpbroker.New(cnf), nil
	}

	if strings.HasPrefix(cnf.Broker, "amqps://") {
		return amqpbroker.New(cnf), nil
	}

	if strings.HasPrefix(cnf.Broker, "redis://") {
		parts := strings.Split(cnf.Broker, "redis://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis broker connection string should be in format redis://host:port, instead got %s",
				cnf.Broker,
			)
		}

		redisHost, redisPassword, redisDB, err := ParseRedisURL(cnf.Broker)
		if err != nil {
			return nil, err
		}
		return redisbroker.New(cnf, redisHost, redisPassword, "", redisDB), nil
	}

	if strings.HasPrefix(cnf.Broker, "redis+socket://") {
		redisSocket, redisPassword, redisDB, err := ParseRedisSocketURL(cnf.Broker)
		if err != nil {
			return nil, err
		}

		return redisbroker.New(cnf, "", redisPassword, redisSocket, redisDB), nil
	}

	if strings.HasPrefix(cnf.Broker, "eager") {
		return eagerbroker.New(), nil
	}

	if strings.HasPrefix(cnf.Broker, "https://sqs") {
		return sqsbroker.New(cnf), nil
	}

	if strings.HasPrefix(cnf.Broker, "cmq://") {
		opt, err := ParseCMQURL(cnf.Broker)
		if err != nil {
			return nil, err
		}
		cnf.Broker = "cmq"
		return cmqbroker.New(cnf, opt), nil
	}

	return nil, fmt.Errorf("Factory failed with broker URL: %v", cnf.Broker)
}

// BackendFactory creates a new object of backends.Interface
// Currently supported backends are AMQP/S and Memcache
func BackendFactory(cnf *config.Config) (backendiface.Backend, error) {
	if strings.HasPrefix(cnf.ResultBackend, "amqp://") {
		return amqpbackend.New(cnf), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "amqps://") {
		return amqpbackend.New(cnf), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "memcache://") {
		parts := strings.Split(cnf.ResultBackend, "memcache://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Memcache result backend connection string should be in format memcache://server1:port,server2:port, instead got %s",
				cnf.ResultBackend,
			)
		}
		servers := strings.Split(parts[1], ",")
		return memcachebackend.New(cnf, servers), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "redis://") {
		redisHost, redisPassword, redisDB, err := ParseRedisURL(cnf.ResultBackend)
		if err != nil {
			return nil, err
		}

		return redisbackend.New(cnf, redisHost, redisPassword, "", redisDB), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "redis+socket://") {
		redisSocket, redisPassword, redisDB, err := ParseRedisSocketURL(cnf.ResultBackend)
		if err != nil {
			return nil, err
		}

		return redisbackend.New(cnf, "", redisPassword, redisSocket, redisDB), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "mongodb://") {
		return mongobackend.New(cnf), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "eager") {
		return eagerbackend.New(), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "https://dynamodb") {
		return dynamobackend.New(cnf), nil
	}

	return nil, fmt.Errorf("Factory failed with result backend: %v", cnf.ResultBackend)
}

// ParseRedisURL ...
func ParseRedisURL(url string) (host, password string, db int, err error) {
	// redis://pwd@host/db

	var u *neturl.URL
	u, err = neturl.Parse(url)
	if err != nil {
		return
	}
	if u.Scheme != "redis" {
		err = errors.New("No redis scheme found")
		return
	}

	if u.User != nil {
		var exists bool
		password, exists = u.User.Password()
		if !exists {
			password = u.User.Username()
		}
	}

	host = u.Host

	parts := strings.Split(u.Path, "/")
	if len(parts) == 1 {
		db = 0 //default redis db
	} else {
		db, err = strconv.Atoi(parts[1])
		if err != nil {
			db, err = 0, nil //ignore err here
		}
	}

	return
}

func ParseCMQURL(url string) (opt *cmq.Options, err error) {
	// cmq://secret_id:secret_key@region?net_env=[wan|lan]
	// wan wide area net
	// lan local area net
	var u *neturl.URL
	u, err = neturl.Parse(url)
	if err != nil {
		return
	}

	if u.Scheme != "cmq" {
		err = errors.New("not cmq scheme found")
		return
	}

	var c *cmq.Credential
	if u.User != nil {
		password, exists := u.User.Password()
		if !exists {
			err = errors.New("secret_key is empty")
			return
		}
		c = &cmq.Credential{
			SecretId:  u.User.Username(),
			SecretKey: password,
		}
	}

	if c == nil {
		err = errors.New("cmq credential is empty")
		return
	}

	netEnv := u.Query().Get("net_env")
	if netEnv != "wan" && netEnv != "lan" {
		err = errors.New("cmq net env must be [wan|lan]")
		return
	}

	return &cmq.Options{
		Region:     u.Host,
		Credential: c,
		NetEnv:     netEnv,
	}, nil
}

// ParseRedisSocketURL extracts Redis connection options from a URL with the
// redis+socket:// scheme. This scheme is not standard (or even de facto) and
// is used as a transitional mechanism until the the config package gains the
// proper facilities to support socket-based connections.
func ParseRedisSocketURL(url string) (path, password string, db int, err error) {
	parts := strings.Split(url, "redis+socket://")
	if parts[0] != "" {
		err = errors.New("No redis scheme found")
		return
	}

	// redis+socket://password@/path/to/file.soc:/db

	if len(parts) != 2 {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}

	remainder := parts[1]

	// Extract password if any
	parts = strings.SplitN(remainder, "@", 2)
	if len(parts) == 2 {
		password = parts[0]
		remainder = parts[1]
	} else {
		remainder = parts[0]
	}

	// Extract path
	parts = strings.SplitN(remainder, ":", 2)
	path = parts[0]
	if path == "" {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}
	if len(parts) == 2 {
		remainder = parts[1]
	}

	// Extract DB if any
	parts = strings.SplitN(remainder, "/", 2)
	if len(parts) == 2 {
		db, _ = strconv.Atoi(parts[1])
	}

	return
}
