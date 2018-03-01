package integration_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/suite"
)

type EagerIntegrationTestSuite struct {
	suite.Suite

	srv    *machinery.Server
	called float64
}

func TestEagerIntegrationTestSuite(t *testing.T) {
	suite.Run(t, &EagerIntegrationTestSuite{})
}

func (s *EagerIntegrationTestSuite) SetupSuite() {
	var err error

	// init server
	cnf := config.Config{
		Broker:        "eager",
		ResultBackend: "eager",
	}
	s.srv, err = machinery.NewServer(&cnf)
	s.Nil(err)
	s.NotNil(s.srv)

	// register task
	s.called = 0
	s.srv.RegisterTask("float_called", func(i float64) (float64, error) {
		s.called = i
		return s.called, nil
	})

	s.srv.RegisterTask("float_result", func(i float64) (float64, error) {
		return i + 100.0, nil
	})

	s.srv.RegisterTask("int_result", func(i int64) (int64, error) {
		return i + 100, nil
	})
}

func (s *EagerIntegrationTestSuite) TestCalled() {
	_, err := s.srv.SendTask(&tasks.Signature{
		Name: "float_called",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: 100.0,
			},
		},
	})

	s.Nil(err)
	s.Equal(100.0, s.called)
}

func (s *EagerIntegrationTestSuite) TestSuccessResult() {
	// float64
	{
		asyncResult, err := s.srv.SendTask(&tasks.Signature{
			Name: "float_result",
			Args: []tasks.Arg{
				{
					Type:  "float64",
					Value: 100.0,
				},
			},
		})

		s.NotNil(asyncResult)
		s.Nil(err)

		s.True(asyncResult.GetState().IsCompleted())
		s.True(asyncResult.GetState().IsSuccess())

		results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
		if s.NoError(err) {
			if len(results) != 1 {
				s.T().Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
			}

			s.Equal(reflect.Float64, results[0].Kind())
			if results[0].Kind() == reflect.Float64 {
				s.Equal(200.0, results[0].Float())
			}
		}
	}

	// int
	{
		asyncResult, err := s.srv.SendTask(&tasks.Signature{
			Name: "int_result",
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 100,
				},
			},
		})

		s.NotNil(asyncResult)
		s.Nil(err)

		s.True(asyncResult.GetState().IsCompleted())
		s.True(asyncResult.GetState().IsSuccess())

		results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
		if s.NoError(err) {
			if len(results) != 1 {
				s.T().Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
			}

			s.Equal(reflect.Int64, results[0].Kind())
			if results[0].Kind() == reflect.Int64 {
				s.Equal(int64(200), results[0].Int())
			}
		}
	}
}
