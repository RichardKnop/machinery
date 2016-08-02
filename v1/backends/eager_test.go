package backends_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/stretchr/testify/suite"
)

type EagerBackendTestSuite struct {
	suite.Suite

	backend backends.Backend
	st      []*signatures.TaskSignature
	groups  []struct {
		id    string
		tasks []string
	}
}

func (s *EagerBackendTestSuite) SetupSuite() {
	// prepare common test data
	s.backend = backends.NewEagerBackend()

	// 2 non-group state
	s.st = []*signatures.TaskSignature{
		{UUID: "1"},
		{UUID: "2"},
		{UUID: "3"},
		{UUID: "4"},
		{UUID: "5"},
		{UUID: "6"},
	}

	for _, t := range s.st {
		s.backend.SetStatePending(t)
	}

	// groups
	s.groups = []struct {
		id    string
		tasks []string
	}{
		{"group1", []string{"1-3", "1-4"}},
		{"group2", []string{"2-1", "2-2", "2-3"}},
		{"group3", []string(nil)},
		{"group4", []string{"4-1", "4-2", "4-3", "4-4"}},
		{"group5", []string{"5-1", "5-2"}},
	}

	for _, g := range s.groups {
		for _, t := range g.tasks {
			sig := &signatures.TaskSignature{
				UUID:           t,
				GroupUUID:      g.id,
				GroupTaskCount: len(g.tasks),
			}
			s.st = append(s.st, sig)

			// default state is pending
			s.backend.SetStatePending(sig)
		}

		s.Nil(s.backend.InitGroup(g.id, g.tasks))
	}

	// prepare for TestInitGroup
	s.Nil(s.backend.PurgeGroupMeta(s.groups[4].id))
}

//
// Test Cases
//

func (s *EagerBackendTestSuite) TestInitGroup() {
	// group 5
	{
		g := s.groups[4]
		s.Nil(s.backend.InitGroup(g.id, g.tasks))
	}

	// group3 -- nil as task list
	{
		g := s.groups[2]
		s.Nil(s.backend.InitGroup(g.id, g.tasks))
	}
}

func (s *EagerBackendTestSuite) TestGroupCompleted() {
	// group 1
	{
		// all tasks are pending
		g := s.groups[0]
		completed, err := s.backend.GroupCompleted(g.id, len(g.tasks))
		s.False(completed)
		s.Nil(err)

		// make these tasks success
		for _, id := range g.tasks {
			t := s.getTaskSignature(id)
			s.NotNil(t)
			if t == nil {
				break
			}

			s.backend.SetStateSuccess(t, nil)
		}

		completed, err = s.backend.GroupCompleted(g.id, len(g.tasks))
		s.True(completed)
		s.Nil(err)
	}

	// group 2
	{
		g := s.groups[1]

		completed, err := s.backend.GroupCompleted(g.id, len(g.tasks))
		s.False(completed)
		s.Nil(err)

		// make these tasks failure
		for _, id := range g.tasks {
			t := s.getTaskSignature(id)
			s.NotNil(t)
			if t == nil {
				break
			}

			s.backend.SetStateFailure(t, "just a test")
		}

		completed, err = s.backend.GroupCompleted(g.id, len(g.tasks))
		s.True(completed)
		s.Nil(err)
	}

	{
		// call on a not-existed group
		completed, err := s.backend.GroupCompleted("", 0)
		s.False(completed)
		s.NotNil(err)
	}
}

func (s *EagerBackendTestSuite) TestGroupTaskStates() {
	// group 4
	{
		g := s.groups[3]

		// set failure state with taskUUID as error message
		for _, id := range g.tasks {
			t := s.getTaskSignature(id)
			s.NotNil(t)
			if t == nil {
				break
			}

			s.backend.SetStateFailure(t, t.UUID)
		}

		// get states back
		ts, err := s.backend.GroupTaskStates(g.id, len(g.tasks))
		s.NotNil(ts)
		s.Nil(err)
		if ts != nil {
			for _, t := range ts {
				s.Equal(t.TaskUUID, t.Error)
			}
		}
	}

	{
		// call on a not-existed group
		ts, err := s.backend.GroupTaskStates("", 0)
		s.Nil(ts)
		s.NotNil(err)
	}
}

func (s *EagerBackendTestSuite) TestSetStatePending() {
	// task 1
	{
		t := s.st[0]

		// change this state to receiving
		s.backend.SetStateReceived(t)

		// change it back to pending
		s.backend.SetStatePending(t)

		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(backends.PendingState, st.State)
		}
	}
}

func (s *EagerBackendTestSuite) TestSetStateReceived() {
	// task2
	{
		t := s.st[1]
		s.backend.SetStateReceived(t)
		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(backends.ReceivedState, st.State)
		}
	}
}

func (s *EagerBackendTestSuite) TestSetStateStarted() {
	// task3
	{
		t := s.st[2]
		s.backend.SetStateStarted(t)
		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(backends.StartedState, st.State)
		}
	}
}

func (s *EagerBackendTestSuite) TestSetStateSuccess() {

	// task4
	{
		t := s.st[3]
		result := &backends.TaskResult{Type: "float64", Value: float64(300.0)}
		s.backend.SetStateSuccess(t, result)
		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(backends.SuccessState, st.State)
			s.Equal(result, st.Result)
		}
	}
}

func (s *EagerBackendTestSuite) TestSetStateFailure() {
	// task5
	{
		t := s.st[4]
		s.backend.SetStateFailure(t, "error")
		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(backends.FailureState, st.State)
			s.Equal("error", st.Error)
		}
	}
}

func (s *EagerBackendTestSuite) TestGetState() {
	// get something not existed -- empty string
	st, err := s.backend.GetState("")
	s.Nil(st)
	s.NotNil(err)
}

func (s *EagerBackendTestSuite) TestPurgeState() {
	// task6
	{
		t := s.st[5]
		st, err := s.backend.GetState(t.UUID)
		s.NotNil(st)
		s.Nil(err)

		// purge it
		s.Nil(s.backend.PurgeState(t.UUID))

		// should be not found
		st, err = s.backend.GetState(t.UUID)
		s.Nil(st)
		s.NotNil(err)
	}

	{
		// purge a not-existed state
		s.NotNil(s.backend.PurgeState(""))
	}
}

func (s *EagerBackendTestSuite) TestPurgeGroupMeta() {
	// group4
	{
		g := s.groups[3]
		ts, err := s.backend.GroupTaskStates(g.id, len(g.tasks))
		s.NotNil(ts)
		s.Nil(err)

		// purge group
		s.Nil(s.backend.PurgeGroupMeta(g.id))

		// should be not found
		ts, err = s.backend.GroupTaskStates(g.id, len(g.tasks))
		s.Nil(ts)
		s.NotNil(err)
	}

	{
		// purge a not-existed group
		s.NotNil(s.backend.PurgeGroupMeta(""))
	}
}

//
// internal method
//
func (s *EagerBackendTestSuite) getTaskSignature(taskUUID string) *signatures.TaskSignature {
	for _, v := range s.st {
		if v.UUID == taskUUID {
			return v
		}
	}

	return nil
}

func TestEagerBackendMain(t *testing.T) {
	suite.Run(t, &EagerBackendTestSuite{})
}
