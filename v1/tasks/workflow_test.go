package tasks_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/suite"
)

type workflowSuite struct {
	suite.Suite
	task1 *tasks.Signature
	task2 *tasks.Signature
	task3 *tasks.Signature
	task4 *tasks.Signature
}

func TestWorkflowSuite(t *testing.T) {
	suite.Run(t, new(workflowSuite))
}

func (s *workflowSuite) SetupTest() {
	s.task1 = &tasks.Signature{
		Name: "foo",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: interface{}(1),
			},
			{
				Type:  "float64",
				Value: interface{}(1),
			},
		},
	}

	s.task2 = &tasks.Signature{
		Name: "bar",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: interface{}(5),
			},
			{
				Type:  "float64",
				Value: interface{}(6),
			},
		},
	}

	s.task3 = &tasks.Signature{
		Name: "qux",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: interface{}(4),
			},
		},
	}

	s.task4 = &tasks.Signature{
		Name: "box",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: interface{}(7),
			},
		},
	}
}

func (s *workflowSuite) TestNewChain() {
	chain, err := tasks.NewChain(s.task1, s.task2, s.task3)
	s.Nil(err)

	firstTask := chain.Tasks[0]

	s.Equal("foo", firstTask.Name)
	s.Equal("bar", firstTask.OnSuccess[0].Name)
	s.Equal("qux", firstTask.OnSuccess[0].OnSuccess[0].Name)
}

func (s *workflowSuite) TestNewGroup() {
	group, err := tasks.NewGroup(s.task1, s.task2, s.task3)
	s.Nil(err)
	s.Equal(len(group.Tasks), 3)
	for _, task := range group.Tasks {
		s.Equal(task.GroupUUID, group.GroupUUID)
		s.Equal(task.GroupTaskCount, len(group.Tasks))
	}
}

func (s *workflowSuite) TestNewChord() {
	group, err := tasks.NewGroup(s.task1, s.task2)
	s.Nil(err)

	chord, err := tasks.NewChord(group, s.task3)
	s.Nil(err)
	s.Equal(chord.Callback, s.task3)
	s.Nil(chord.ErrorCallback)

	for _, task := range group.Tasks {
		s.Equal(task.ChordCallback, s.task3)
		s.Nil(task.ChordErrorCallback)
	}

}

func (s *workflowSuite) TestNewChordWithError() {
	group, err := tasks.NewGroup(s.task1, s.task2)
	s.Nil(err)

	errChord, err := tasks.NewChordWithError(group, s.task3, s.task4)
	s.Nil(err)
	s.Equal(errChord.Callback, s.task3)
	s.Equal(errChord.ErrorCallback, s.task4)

	for _, task := range group.Tasks {
		s.Equal(task.ChordCallback, s.task3)
		s.Equal(task.ChordErrorCallback, s.task4)
	}
}
