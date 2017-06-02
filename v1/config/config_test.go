package config_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

func TestRefreshConfig(t *testing.T) {
	config.Reset()

	cnf := config.NewFromEnvironment(true, false)

	config.Refresh(&config.Config{Broker: "foo"})
	assert.Equal(t, "foo", cnf.Broker)

	config.Refresh(&config.Config{Broker: "bar"})
	assert.Equal(t, "bar", cnf.Broker)
}
