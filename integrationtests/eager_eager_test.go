package integrationtests

import (
	"reflect"
	"testing"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
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
	_, err := s.srv.SendTask(&signatures.TaskSignature{
		Name: "float_called",
		Args: []signatures.TaskArg{
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
		result, err := s.srv.SendTask(&signatures.TaskSignature{
			Name: "float_result",
			Args: []signatures.TaskArg{
				{
					Type:  "float64",
					Value: 100.0,
				},
			},
		})

		s.NotNil(result)
		s.Nil(err)
		if result != nil {
			s.True(result.GetState().IsCompleted())
			s.True(result.GetState().IsSuccess())

			ret, err := result.Get()
			s.Nil(err)
			s.Equal(reflect.Float64, ret.Kind())
			if ret.Kind() == reflect.Float64 {
				s.Equal(200.0, ret.Float())
			}
		}
	}

	// int
	{
		result, err := s.srv.SendTask(&signatures.TaskSignature{
			Name: "int_result",
			Args: []signatures.TaskArg{
				{
					Type:  "int64",
					Value: 100,
				},
			},
		})

		s.NotNil(result)
		s.Nil(err)
		if result != nil {
			s.True(result.GetState().IsCompleted())
			s.True(result.GetState().IsSuccess())

			ret, err := result.Get()
			s.Nil(err)
			s.Equal(reflect.Int64, ret.Kind())
			if ret.Kind() == reflect.Int64 {
				s.Equal(int64(200), ret.Int())
			}
		}

	}
}
