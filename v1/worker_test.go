package machinery

import (
	"testing"

	backendmocks "github.com/RichardKnop/machinery/v1/backends/iface/mocks"
	"github.com/RichardKnop/machinery/v1/iface/mocks"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/machinery/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestRedactURL(t *testing.T) {
	t.Parallel()

	broker := "amqp://guest:guest@localhost:5672"
	redactedURL := machinery.RedactURL(broker)
	assert.Equal(t, "amqp://localhost:5672", redactedURL)
}

func TestPreConsumeHandler(t *testing.T) {
	t.Parallel()
	worker := &machinery.Worker{}

	worker.SetPreConsumeHandler(SamplePreConsumeHandler)
	assert.True(t, worker.PreConsumeHandler())
}

func SamplePreConsumeHandler(w *machinery.Worker) bool {
	return true
}

const (
	groupUUID      = "APPLE"
	groupTaskCount = 17
)

type processChordSuite struct {
	suite.Suite
	serverMock  *mocks.Server
	backendMock *backendmocks.Backend

	chordSuccess *tasks.Signature
	chordError   *tasks.Signature
}

func TestProcessChordSuite(t *testing.T) {
	suite.Run(t, new(processChordSuite))
}

func (s *processChordSuite) SetupTest() {
	s.backendMock = &backendmocks.Backend{}
	s.serverMock = &mocks.Server{}
	s.chordSuccess = &tasks.Signature{Name: "SuccessChord"}
	s.chordError = &tasks.Signature{Name: "ErrorChord"}
}

func (s *processChordSuite) AfterTest(suiteName, testName string) {
	s.backendMock.AssertExpectations(s.T())
	s.serverMock.AssertExpectations(s.T())
}

func (s *processChordSuite) TestNoChords() {
	s.Nil(processChords(s.serverMock, nil, false))

	sigWithoutChord := &tasks.Signature{Name: "BANANA"}
	s.Nil(processChords(s.serverMock, sigWithoutChord, false))
}

func (s *processChordSuite) TestErrorChord() {
	// this task will fail out
	taskStates := []*tasks.TaskState{
		{
			State: tasks.StateFailure,
			Error: "PEACH",
		},
	}

	s.backendMock.On("GroupCompleted", groupUUID, groupTaskCount).Return(true, nil)
	s.backendMock.On("TriggerChord", groupUUID).Return(true, nil)
	s.backendMock.On("GroupTaskStates", groupUUID, groupTaskCount).Return(taskStates, nil)

	s.serverMock.On("GetBackend").Return(s.backendMock)
	s.serverMock.On("SendTask", s.chordError).Return(nil, nil)

	sig := &tasks.Signature{
		Name:               "BANANA",
		GroupUUID:          groupUUID,
		GroupTaskCount:     groupTaskCount,
		ChordCallback:      s.chordSuccess,
		ChordErrorCallback: s.chordError,
	}

	s.Nil(processChords(s.serverMock, sig, false))
}

func (s *processChordSuite) TestSuccessChord() {
	// this task will fail out
	taskStates := []*tasks.TaskState{
		{
			State: tasks.StateSuccess,
			Results: []*tasks.TaskResult{
				{Type: "string", Value: "GRAPE"},
			},
		},
	}

	s.backendMock.On("GroupCompleted", groupUUID, groupTaskCount).Return(true, nil)
	s.backendMock.On("TriggerChord", groupUUID).Return(true, nil)
	s.backendMock.On("GroupTaskStates", groupUUID, groupTaskCount).Return(taskStates, nil)

	s.serverMock.On("GetBackend").Return(s.backendMock)
	s.serverMock.On("SendTask", s.chordSuccess).Return(nil, nil)

	sig := &tasks.Signature{
		Name:               "BANANA",
		GroupUUID:          groupUUID,
		GroupTaskCount:     groupTaskCount,
		ChordCallback:      s.chordSuccess,
		ChordErrorCallback: s.chordError,
	}

	s.Nil(processChords(s.serverMock, sig, false))
}

func (s *processChordSuite) TestSuccessWithoutChord() {
	// this task will fail out
	taskStates := []*tasks.TaskState{
		{
			State: tasks.StateSuccess,
			Results: []*tasks.TaskResult{
				{Type: "string", Value: "GRAPE"},
			},
		},
	}

	s.backendMock.On("GroupCompleted", groupUUID, groupTaskCount).Return(true, nil)
	s.backendMock.On("TriggerChord", groupUUID).Return(true, nil)
	s.backendMock.On("GroupTaskStates", groupUUID, groupTaskCount).Return(taskStates, nil)

	s.serverMock.On("GetBackend").Return(s.backendMock)
	// no SendTask should get called on the server as there is no Chord

	sig := &tasks.Signature{
		Name:               "BANANA",
		GroupUUID:          groupUUID,
		GroupTaskCount:     groupTaskCount,
		ChordErrorCallback: s.chordError,
	}

	s.Nil(processChords(s.serverMock, sig, false))
}

func (s *processChordSuite) TestErrorWithoutChord() {
	// this task will fail out
	taskStates := []*tasks.TaskState{
		{
			State: tasks.StateFailure,
			Error: "PEACH",
		},
	}

	s.backendMock.On("GroupCompleted", groupUUID, groupTaskCount).Return(true, nil)
	s.backendMock.On("TriggerChord", groupUUID).Return(true, nil)
	s.backendMock.On("GroupTaskStates", groupUUID, groupTaskCount).Return(taskStates, nil)

	s.serverMock.On("GetBackend").Return(s.backendMock)
	// No send task

	sig := &tasks.Signature{
		Name:           "BANANA",
		GroupUUID:      groupUUID,
		GroupTaskCount: groupTaskCount,
		ChordCallback:  s.chordSuccess,
	}

	s.Nil(processChords(s.serverMock, sig, false))
}

func (s *processChordSuite) TestSuccessChordAMQPBackend() {
	// this task will fail out
	taskStates := []*tasks.TaskState{
		{
			State: tasks.StateSuccess,
			Results: []*tasks.TaskResult{
				{Type: "string", Value: "GRAPE"},
			},
		},
	}

	s.backendMock.On("GroupCompleted", groupUUID, groupTaskCount).Return(true, nil)
	s.backendMock.On("TriggerChord", groupUUID).Return(true, nil)
	s.backendMock.On("GroupTaskStates", groupUUID, groupTaskCount).Return(taskStates, nil)
	s.backendMock.On("PurgeGroupMeta", groupUUID).Return(nil)

	s.serverMock.On("GetBackend").Return(s.backendMock)
	s.serverMock.On("SendTask", s.chordSuccess).Return(nil, nil)

	sig := &tasks.Signature{
		Name:               "BANANA",
		GroupUUID:          groupUUID,
		GroupTaskCount:     groupTaskCount,
		ChordCallback:      s.chordSuccess,
		ChordErrorCallback: s.chordError,
	}

	s.Nil(processChords(s.serverMock, sig, true))
}
