package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeouts(t *testing.T) {
	c := NewAMQPConnector("", nil)
	assert.Equal(t, c.exchangeMaxRetries, 3)
	assert.Equal(t, c.exchangeRetryTimeout, time.Second)
	assert.Equal(t, c.connManager.connectionMaxRetries, 3)
	assert.Equal(t, c.connManager.connectionRetryTimeout, 5*time.Second)
	c = NewAMQPConnector("", nil,
		WithAMQPConnectionMaxRetries(10),
		WithAMQPConnectionRetryTimeout(time.Minute),
		WithAMQPExchangeMaxRetries(20),
		WithAMQPExchangeRetryTimeout(time.Hour),
	)
	assert.Equal(t, c.exchangeMaxRetries, 20)
	assert.Equal(t, c.exchangeRetryTimeout, time.Hour)
	assert.Equal(t, c.connManager.connectionMaxRetries, 10)
	assert.Equal(t, c.connManager.connectionRetryTimeout, time.Minute)
}
