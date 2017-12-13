package backends_test

import (
	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	testConf *config.Config
)

func init() {
}

func TestNewDynamoDBBackend(t *testing.T) {
	backend := backends.NewDynamoDBBackend(testConf)
	assert.IsType(t, &backends.DynamoDBBackend{}, backend)
}

func TestInitGroup(t *testing.T) {
	groupUUID := "testGroupUUID"
	taskUUIDs = []string{"testTaskUUID1", "testTaskUUID2", "testTaskUUID3"}
	err := backends.TestDynamoDBBackend.InitGroup(groupUUID, taskUUIDs)
	assert.Nil(t, err)
}
