package tcp

import "encoding/json"

const (
	// RequestKeyAdd is a request key for consumer's Add function
	RequestKeyAdd = "add"

	// RequestKeyRemove is a request key for consumer's Remove function
	RequestKeyRemove = "remove"

	// RequestKeyAssignments is a request key for consumer's Assignment function
	RequestKeyAssignments = "assignments"

	// RequestKeyOffset is a request key for consumer's Offset function
	RequestKeyOffset = "offset"

	// RequestKeyCommit is a request key for consumer's Commit function
	RequestKeyCommit = "commit"

	// RequestKeySetOffset is a request key for consumer's SetOffset function
	RequestKeySetOffset = "setoffset"

	// RequestKeyLag is a request key for consumer's Lag function
	RequestKeyLag = "lag"
)

type clientRequest struct {
	Key  string
	Data interface{}
}

type request struct {
	Key  string
	Data json.RawMessage
}

// Response defines the response message format to exchange data.
type Response struct {
	// Success is a flag whether the request associated with this response succeeded.
	Success bool

	// Message is any description or error message associated with this response.
	Message string

	// Data is any additional payload associated with this response.
	Data interface{}
}

// NewResponse creates a new Response.
func NewResponse(success bool, message string, data interface{}) *Response {
	return &Response{
		Success: success,
		Message: message,
		Data:    data,
	}
}

type topicPartition struct {
	Topic     string
	Partition int32
}

type topicPartitionOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}
