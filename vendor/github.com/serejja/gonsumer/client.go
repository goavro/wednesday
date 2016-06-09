package gonsumer

import "github.com/serejja/kafka-client"

// Client is an interface responsible for low-level Kafka interaction.
// The only supported implmentation now is github.com/serejja/kafka-client.
// One other implementation, MockClient, is used for testing purposes.
type Client interface {
	// Fetch is responsible for fetching messages for given topic, partition and offset from Kafka broker.
	// Leader change handling happens inside Fetch and is hidden from user so he should not handle such cases.
	// Returns a fetch response and error if it occurred.
	Fetch(topic string, partition int32, offset int64) (*client.FetchResponse, error)

	// GetAvailableOffset issues an offset request to a specified topic and partition with a given offset time.
	// Returns an offet for given topic, partition and offset time and an error if it occurs.
	GetAvailableOffset(topic string, partition int32, offsetTime int64) (int64, error)

	// GetOffset gets the latest committed offset for a given group, topic and partition from Kafka.
	// Returns an offset and an error if it occurs.
	GetOffset(group string, topic string, partition int32) (int64, error)

	// CommitOffset commits the given offset for a given group, topic and partition to Kafka.
	// Returns an error if commit was unsuccessful.
	CommitOffset(group string, topic string, partition int32, offset int64) error
}
