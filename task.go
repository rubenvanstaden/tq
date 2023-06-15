package tq

type Result struct {
	Error error
	Value interface{}
}

type Task struct {

	// The key of the task function that will execute the job.
	Key string `json:"key"`

	// Holds the serialized signature.
	Payload []byte `json:"payload"`

	// Maximum retry count for task execution to succeed.
	RetryCount int `json:"retry_count,string"`

	// Time duration between each retry attempt.
	RetryTimeout int `json:"retry_timeout,string"`

	// If task processing doesn't complete within the timeout, the task will be retried
	Timeout int64 `json:"timeout,string"`
}
