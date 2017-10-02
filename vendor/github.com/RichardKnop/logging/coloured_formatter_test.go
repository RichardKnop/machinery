package logging_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/RichardKnop/logging"
	"github.com/stretchr/testify/assert"
)

func TestColouredFormatter(t *testing.T) {
	t.Parallel()

	var (
		out, errOut = bytes.NewBuffer([]byte{}), bytes.NewBuffer([]byte{})
		logger      = logging.New(out, errOut, new(logging.ColouredFormatter))
		now         time.Time
		actual      []byte
		expected    string
		err         error
	)

	// Test logger.Info
	now = time.Now()
	logger[logging.INFO].Print("Test logger.Print")
	actual, err = ioutil.ReadAll(out)
	if err != nil {
		log.Fatal(err)
	}
	expected = fmt.Sprintf(
		"\x1b[0;94mINFO: %s coloured_formatter_test.go:29 Test logger.Print\x1b[0m\n",
		now.Format("2006/01/02 15:04:05"),
	)
	assert.Equal(t, expected, string(actual))

	// Test logger.Infof
	now = time.Now()
	logger[logging.INFO].Printf("Test %s.%s", "logger", "Printf")
	actual, err = ioutil.ReadAll(out)
	if err != nil {
		log.Fatal(err)
	}
	expected = fmt.Sprintf(
		"\x1b[0;94mINFO: %s coloured_formatter_test.go:42 Test logger.Printf\x1b[0m\n",
		now.Format("2006/01/02 15:04:05"),
	)
	assert.Equal(t, expected, string(actual))

	// Test logger.Warning
	now = time.Now()
	logger[logging.WARNING].Print("Test logger.Print")
	actual, err = ioutil.ReadAll(out)
	if err != nil {
		log.Fatal(err)
	}
	expected = fmt.Sprintf(
		"\x1b[0;95mWARNING: %s coloured_formatter_test.go:55 Test logger.Print\x1b[0m\n",
		now.Format("2006/01/02 15:04:05"),
	)
	assert.Equal(t, expected, string(actual))

	// Test logger.Warningf
	now = time.Now()
	logger[logging.WARNING].Printf("Test %s.%s", "logger", "Printf")
	actual, err = ioutil.ReadAll(out)
	if err != nil {
		log.Fatal(err)
	}
	expected = fmt.Sprintf(
		"\x1b[0;95mWARNING: %s coloured_formatter_test.go:68 Test logger.Printf\x1b[0m\n",
		now.Format("2006/01/02 15:04:05"),
	)
	assert.Equal(t, expected, string(actual))

	// Test logger.Error
	now = time.Now()
	logger[logging.ERROR].Print("Test logger.Print")
	actual, err = ioutil.ReadAll(errOut)
	if err != nil {
		log.Fatal(err)
	}
	expected = fmt.Sprintf(
		"\x1b[0;91mERROR: %s coloured_formatter_test.go:81 Test logger.Print\x1b[0m\n",
		now.Format("2006/01/02 15:04:05"),
	)
	assert.Equal(t, expected, string(actual))

	// Test logger.Errorf
	now = time.Now()
	logger[logging.ERROR].Printf("Test %s.%s", "logger", "Printf")
	actual, err = ioutil.ReadAll(errOut)
	if err != nil {
		log.Fatal(err)
	}
	expected = fmt.Sprintf(
		"\x1b[0;91mERROR: %s coloured_formatter_test.go:94 Test logger.Printf\x1b[0m\n",
		now.Format("2006/01/02 15:04:05"),
	)
	assert.Equal(t, expected, string(actual))

	// Test logger.Fatal
	now = time.Now()
	logger[logging.FATAL].Print("Test logger.Print")
	actual, err = ioutil.ReadAll(errOut)
	if err != nil {
		log.Fatal(err)
	}
	expected = fmt.Sprintf(
		"\x1b[0;91mFATAL: %s coloured_formatter_test.go:107 Test logger.Print\x1b[0m\n",
		now.Format("2006/01/02 15:04:05"),
	)
	assert.Equal(t, expected, string(actual))

	// Test logger.Fatalf
	now = time.Now()
	logger[logging.FATAL].Printf("Test %s.%s", "logger", "Printf")
	actual, err = ioutil.ReadAll(errOut)
	if err != nil {
		log.Fatal(err)
	}
	expected = fmt.Sprintf(
		"\x1b[0;91mFATAL: %s coloured_formatter_test.go:120 Test logger.Printf\x1b[0m\n",
		now.Format("2006/01/02 15:04:05"),
	)
	assert.Equal(t, expected, string(actual))
}
