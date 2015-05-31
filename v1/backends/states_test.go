package backends

import "testing"

func TestTaskStateGroupIsSuccess(t *testing.T) {
	taskStateGroup := TaskStateGroup{
		GroupUUID: "groupUUID",
		States: map[string]TaskState{
			"taskUUID1": TaskState{
				TaskUUID: "taskUUID1",
				State:    ReceivedState,
			},
			"taskUUID2": TaskState{
				TaskUUID: "taskUUID2",
				State:    PendingState,
			},
		},
	}

	if taskStateGroup.IsSuccess() {
		t.Errorf("taskStateGroup.IsSuccess() = true, should be false. %v",
			taskStateGroup)
	}

	taskStateGroup.States["taskUUID1"] = TaskState{
		TaskUUID: "taskUUID1",
		State:    SuccessState,
	}

	if taskStateGroup.IsSuccess() {
		t.Errorf("taskStateGroup.IsSuccess() = true, should be false. %v",
			taskStateGroup)
	}

	taskStateGroup.States["taskUUID2"] = TaskState{
		TaskUUID: "taskUUID2",
		State:    FailureState,
	}

	if taskStateGroup.IsSuccess() {
		t.Errorf("taskStateGroup.IsSuccess() = true, should be false. %v",
			taskStateGroup)
	}

	taskStateGroup.States["taskUUID2"] = TaskState{
		TaskUUID: "taskUUID2",
		State:    SuccessState,
	}

	if !taskStateGroup.IsSuccess() {
		t.Errorf("taskStateGroup.IsSuccess() = false, should be true. %v",
			taskStateGroup)
	}
}

func TestTaskStateGroupIsFailure(t *testing.T) {
	taskStateGroup := TaskStateGroup{
		GroupUUID: "groupUUID",
		States: map[string]TaskState{
			"taskUUID1": TaskState{
				TaskUUID: "taskUUID1",
				State:    ReceivedState,
			},
			"taskUUID2": TaskState{
				TaskUUID: "taskUUID2",
				State:    PendingState,
			},
		},
	}

	if taskStateGroup.IsFailure() {
		t.Errorf("taskStateGroup.IsFailure() = true, should be false. %v",
			taskStateGroup)
	}

	taskStateGroup.States["taskUUID1"] = TaskState{
		TaskUUID: "taskUUID1",
		State:    SuccessState,
	}

	if taskStateGroup.IsFailure() {
		t.Errorf("taskStateGroup.IsFailure() = true, should be false. %v",
			taskStateGroup)
	}

	taskStateGroup.States["taskUUID2"] = TaskState{
		TaskUUID: "taskUUID2",
		State:    SuccessState,
	}

	if taskStateGroup.IsFailure() {
		t.Errorf("taskStateGroup.IsFailure() = true, should be false. %v",
			taskStateGroup)
	}

	taskStateGroup.States["taskUUID2"] = TaskState{
		TaskUUID: "taskUUID2",
		State:    FailureState,
	}

	if !taskStateGroup.IsFailure() {
		t.Errorf("taskStateGroup.IsFailure() = false, should be true. %v",
			taskStateGroup)
	}
}

func TestTaskStateGroupIsCompleted(t *testing.T) {
	taskStateGroup := TaskStateGroup{
		GroupUUID: "groupUUID",
		States: map[string]TaskState{
			"taskUUID1": TaskState{
				TaskUUID: "taskUUID1",
				State:    ReceivedState,
			},
			"taskUUID2": TaskState{
				TaskUUID: "taskUUID2",
				State:    PendingState,
			},
		},
	}

	if taskStateGroup.IsCompleted() {
		t.Errorf("taskStateGroup.IsCompleted() = true, should be false. %v",
			taskStateGroup)
	}

	taskStateGroup.States["taskUUID1"] = TaskState{
		TaskUUID: "taskUUID1",
		State:    SuccessState,
	}

	if taskStateGroup.IsCompleted() {
		t.Errorf("taskStateGroup.IsCompleted() = true, should be false. %v",
			taskStateGroup)
	}

	taskStateGroup.States["taskUUID2"] = TaskState{
		TaskUUID: "taskUUID2",
		State:    FailureState,
	}

	if !taskStateGroup.IsCompleted() {
		t.Errorf("taskStateGroup.IsCompleted() = false, should be true. %v",
			taskStateGroup)
	}

	taskStateGroup.States["taskUUID2"] = TaskState{
		TaskUUID: "taskUUID2",
		State:    SuccessState,
	}

	if !taskStateGroup.IsCompleted() {
		t.Errorf("taskStateGroup.IsCompleted() = false, should be true. %v",
			taskStateGroup)
	}
}
