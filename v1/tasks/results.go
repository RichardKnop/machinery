package tasks

// TaskResult represents an actual return value of a processed task
type TaskResult struct {
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}
