package backends

import "testing"

func TestTaskStateIsCompleted(t *testing.T) {
	taskState := &TaskState{
		TaskUUID: "taskUUID",
		State:    PendingState,
	}

	if taskState.IsCompleted() {
		t.Errorf("taskState.IsCompleted() = true, should be false. %v",
			taskState)
	}

	taskState.State = ReceivedState
	if taskState.IsCompleted() {
		t.Errorf("taskState.IsCompleted() = true, should be false. %v",
			taskState)
	}

	taskState.State = StartedState
	if taskState.IsCompleted() {
		t.Errorf("taskState.IsCompleted() = true, should be false. %v",
			taskState)
	}

	taskState.State = SuccessState
	if !taskState.IsCompleted() {
		t.Errorf("taskState.IsCompleted() = false, should be true. %v",
			taskState)
	}

	taskState.State = FailureState
	if !taskState.IsCompleted() {
		t.Errorf("taskState.IsCompleted() = false, should be true. %v",
			taskState)
	}
}
