package gonsumer

// FetchData is a slightly processed FetchResponse to be more user-friendly to use.
type FetchData struct {
	// Messages is a slice of actually fetched messages from Kafka.
	Messages []*MessageAndMetadata

	// HighwaterMarkOffset is an offset in the end of topic/partition this FetchData comes from.
	HighwaterMarkOffset int64

	// Error is an error that occurs both on fetch level or topic/partition level.
	Error error
}

// MessageAndMetadata is a single Kafka message and its metadata.
type MessageAndMetadata struct {
	// Key is a raw message key.
	Key []byte

	// Value is a raw message value.
	Value []byte

	// Topic is a Kafka topic this message comes from.
	Topic string

	// Partition is a Kafka partition this message comes from.
	Partition int32

	// Offset is an offset for this message.
	Offset int64

	// DecodedKey is a message key processed by KeyDecoder.
	DecodedKey interface{}

	// DecodedValue is a message value processed by ValueDecoder.
	DecodedValue interface{}
}
