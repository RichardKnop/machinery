package config_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

const (
	tenBytes = "MACHINERY_"
)

func testFile(lines int) ([]byte, string, func() error, error) {
	b := bytes.NewBuffer([]byte{})
	name := filepath.Join(os.TempDir(), fmt.Sprintf("file_test_%d", lines))
	f, err := os.Create(name)
	if err != nil {
		return nil, "", nil, err
	}

	for i := 0; i < lines; i++ {
		_, err := f.WriteString(tenBytes)
		if err != nil {
			return nil, "", nil, err
		}
		_, err = b.WriteString(tenBytes)
		if err != nil {
			return nil, "", nil, err
		}
	}

	defer f.Close()

	return b.Bytes(), name, func() error {
		return os.Remove(name)
	}, nil
}

func TestReadFromFile(t *testing.T) {
	t.Parallel()

	content, name, closer, err := testFile(config.ConfigMaxSize/len(tenBytes) - 1)
	defer closer()

	data, err := config.ReadFromFile(name)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, content, data)
}

func TestReadFromFile_TooLarge(t *testing.T) {
	t.Parallel()

	content, name, closer, err := testFile(config.ConfigMaxSize/len(tenBytes) + 1)
	defer closer()

	data, err := config.ReadFromFile(name)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(data), config.ConfigMaxSize)
	assert.Equal(t, data, content[0:config.ConfigMaxSize])
}

func TestNewFromYaml(t *testing.T) {
	t.Parallel()

	cnf, err := config.NewFromYaml("testconfig.yml", false)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "broker", cnf.Broker)
	assert.Equal(t, "default_queue", cnf.DefaultQueue)
	assert.Equal(t, "result_backend", cnf.ResultBackend)
	assert.Equal(t, 123456, cnf.ResultsExpireIn)

	assert.Equal(t, "exchange", cnf.AMQP.Exchange)
	assert.Equal(t, "exchange_type", cnf.AMQP.ExchangeType)
	assert.Equal(t, "binding_key", cnf.AMQP.BindingKey)
	assert.Equal(t, 10, cnf.AMQP.QueueDeclareArgs["x-max-priority"])
	assert.Equal(t, "any", cnf.AMQP.QueueBindingArgs["x-match"])
	assert.Equal(t, "png", cnf.AMQP.QueueBindingArgs["image-type"])
	assert.Equal(t, 123, cnf.AMQP.PrefetchCount)

	assert.Equal(t, 123, cnf.SQS.WaitTimeSeconds)
	assert.Equal(t, 456, *cnf.SQS.VisibilityTimeout)

	assert.Equal(t, 12, cnf.Redis.MaxIdle)
	assert.Equal(t, 123, cnf.Redis.MaxActive)
	assert.Equal(t, 456, cnf.Redis.IdleTimeout)
	assert.Equal(t, false, cnf.Redis.Wait)
	assert.Equal(t, 17, cnf.Redis.ReadTimeout)
	assert.Equal(t, 19, cnf.Redis.WriteTimeout)
	assert.Equal(t, 21, cnf.Redis.ConnectTimeout)
	assert.Equal(t, 1001, cnf.Redis.NormalTasksPollPeriod)
	assert.Equal(t, 23, cnf.Redis.DelayedTasksPollPeriod)
	assert.Equal(t, "delayed_tasks_key", cnf.Redis.DelayedTasksKey)
	assert.Equal(t, "master_name", cnf.Redis.MasterName)

	assert.Equal(t, true, cnf.NoUnixSignals)

	assert.Equal(t, "task_states_table", cnf.DynamoDB.TaskStatesTable)
	assert.Equal(t, "group_metas_table", cnf.DynamoDB.GroupMetasTable)
}
