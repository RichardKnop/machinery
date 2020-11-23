package machinery

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	neturl "net/url"

	"github.com/RichardKnop/machinery/v1/config"

	amqpbroker "github.com/RichardKnop/machinery/v1/brokers/amqp"
	eagerbroker "github.com/RichardKnop/machinery/v1/brokers/eager"
	gcppubsubbroker "github.com/RichardKnop/machinery/v1/brokers/gcppubsub"
	brokeriface "github.com/RichardKnop/machinery/v1/brokers/iface"
	redisbroker "github.com/RichardKnop/machinery/v1/brokers/redis"
	sqsbroker "github.com/RichardKnop/machinery/v1/brokers/sqs"

	amqpbackend "github.com/RichardKnop/machinery/v1/backends/amqp"
	dynamobackend "github.com/RichardKnop/machinery/v1/backends/dynamodb"
	eagerbackend "github.com/RichardKnop/machinery/v1/backends/eager"
	backendiface "github.com/RichardKnop/machinery/v1/backends/iface"
	memcachebackend "github.com/RichardKnop/machinery/v1/backends/memcache"
	mongobackend "github.com/RichardKnop/machinery/v1/backends/mongo"
	nullbackend "github.com/RichardKnop/machinery/v1/backends/null"
	redisbackend "github.com/RichardKnop/machinery/v1/backends/redis"

	eagerlock "github.com/RichardKnop/machinery/v1/locks/eager"
	lockiface "github.com/RichardKnop/machinery/v1/locks/iface"
	redislock "github.com/RichardKnop/machinery/v1/locks/redis"
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

	if strings.HasPrefix(cnf.Broker, "redis://") || strings.HasPrefix(cnf.Broker, "rediss://") {
		var scheme string
		if strings.HasPrefix(cnf.Broker, "redis://") {
			scheme = "redis://"
		} else {
			scheme = "rediss://"
		}
		parts := strings.Split(cnf.Broker, scheme)
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis broker connection string should be in format %shost:port, instead got %s", scheme,
				cnf.Broker,
			)
		}
		brokers := strings.Split(parts[1], ",")
		if len(brokers) > 1 {
			return redisbroker.NewGR(cnf, brokers, 0), nil
		} else {
			redisHost, redisPassword, redisDB, err := ParseRedisURL(cnf.Broker)
			if err != nil {
				return nil, err
			}
			return redisbroker.New(cnf, redisHost, redisPassword, "", redisDB), nil
		}
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

	if _, ok := os.LookupEnv("DISABLE_STRICT_SQS_CHECK"); ok {
		//disable SQS name check, so that users can use this with local simulated SQS
		//where sql broker url might not start with https://sqs

		//even when disabling strict SQS naming check, make sure its still a valid http URL
		if strings.HasPrefix(cnf.Broker, "https://") || strings.HasPrefix(cnf.Broker, "http://") {
			return sqsbroker.New(cnf), nil
		}
	} else {
		if strings.HasPrefix(cnf.Broker, "https://sqs") {
			return sqsbroker.New(cnf), nil
		}
	}

	if strings.HasPrefix(cnf.Broker, "gcppubsub://") {
		projectID, subscriptionName, err := ParseGCPPubSubURL(cnf.Broker)
		if err != nil {
			return nil, err
		}
		return gcppubsubbroker.New(cnf, projectID, subscriptionName)
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

	if strings.HasPrefix(cnf.ResultBackend, "redis://") || strings.HasPrefix(cnf.ResultBackend, "rediss://") {
		var scheme string
		if strings.HasPrefix(cnf.ResultBackend, "redis://") {
			scheme = "redis://"
		} else {
			scheme = "rediss://"
		}
		parts := strings.Split(cnf.ResultBackend, scheme)
		addrs := strings.Split(parts[1], ",")
		if len(addrs) > 1 {
			return redisbackend.NewGR(cnf, addrs, 0), nil
		} else {
			redisHost, redisPassword, redisDB, err := ParseRedisURL(cnf.ResultBackend)

			if err != nil {
				return nil, err
			}

			return redisbackend.New(cnf, redisHost, redisPassword, "", redisDB), nil
		}
	}

	if strings.HasPrefix(cnf.ResultBackend, "redis+socket://") {
		redisSocket, redisPassword, redisDB, err := ParseRedisSocketURL(cnf.ResultBackend)
		if err != nil {
			return nil, err
		}

		return redisbackend.New(cnf, "", redisPassword, redisSocket, redisDB), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "mongodb://") ||
		strings.HasPrefix(cnf.ResultBackend, "mongodb+srv://") {
		return mongobackend.New(cnf)
	}

	if strings.HasPrefix(cnf.ResultBackend, "eager") {
		return eagerbackend.New(), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "null") {
		return nullbackend.New(), nil
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
	if u.Scheme != "redis" && u.Scheme != "rediss" {
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

// LockFactory creates a new object of iface.Lock
// Currently supported lock is redis
func LockFactory(cnf *config.Config) (lockiface.Lock, error) {
	if strings.HasPrefix(cnf.Lock, "eager") {
		return eagerlock.New(), nil
	}
	if strings.HasPrefix(cnf.Lock, "redis://") {
		parts := strings.Split(cnf.Lock, "redis://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis broker connection string should be in format redis://host:port, instead got %s",
				cnf.Lock,
			)
		}
		locks := strings.Split(parts[1], ",")
		return redislock.New(cnf, locks, 0, 3), nil
	}

	// Lock is required for periodic tasks to work, therefor return in memory lock in case none is configured
	return eagerlock.New(), nil
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

// ParseGCPPubSubURL Parse GCP Pub/Sub URL
// url: gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME
func ParseGCPPubSubURL(url string) (string, string, error) {
	parts := strings.Split(url, "gcppubsub://")
	if parts[0] != "" {
		return "", "", errors.New("No gcppubsub scheme found")
	}

	if len(parts) != 2 {
		return "", "", fmt.Errorf("gcppubsub scheme should be in format gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME, instead got %s", url)
	}

	remainder := parts[1]

	parts = strings.Split(remainder, "/")
	if len(parts) == 2 {
		if parts[0] == "" {
			return "", "", fmt.Errorf("gcppubsub scheme should be in format gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME, instead got %s", url)
		}
		if parts[1] == "" {
			return "", "", fmt.Errorf("gcppubsub scheme should be in format gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME, instead got %s", url)
		}
		return parts[0], parts[1], nil
	}

	return "", "", fmt.Errorf("gcppubsub scheme should be in format gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME, instead got %s", url)
}
