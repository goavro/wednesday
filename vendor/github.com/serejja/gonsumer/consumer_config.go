package gonsumer

import (
	"github.com/satori/go.uuid"
	"github.com/serejja/kafka-client"
	"time"
)

// ConsumerConfig provides configuration options for both Consumer and PartitionConsumer.
type ConsumerConfig struct {
	// Group is a string that uniquely identifies a set of consumers within the same consumer group.
	Group string

	// ConsumerID is a string that uniquely identifies a consumer within a consumer group.
	// Defaults to a random UUID.
	ConsumerID string

	// KeyDecoder is a function that turns plain bytes into a decoded message key.
	KeyDecoder Decoder

	// ValueDecoder is a function that turns plain bytes into a decoded message value.
	ValueDecoder Decoder

	// AutoOffsetReset defines what to do when there is no committed offset or committed offset is out of range.
	// kafka-client.EarliestTime - automatically reset the offset to the smallest offset.
	// kafka-client.LatestTime - automatically reset the offset to the largest offset.
	// Defaults to kafka-client.EarliestTime.
	AutoOffsetReset int64

	// AutoCommitEnable determines whether the consumer will automatically commit offsets after each batch
	// is finished (e.g. the call to strategy function returns). Turned off by default.
	AutoCommitEnable bool

	// EnableMetrics determines whether the consumer will collect all kinds of metrics to better understand what's
	// going on under the hood. Turned off by default as it may significantly affect performance.
	EnableMetrics bool

	// Backoff between attempts to initialize consumer offset.
	InitOffsetBackoff time.Duration
}

// NewConfig creates a consumer config with sane defaults.
func NewConfig() *ConsumerConfig {
	return &ConsumerConfig{
		Group:             "gonsumer-group",
		ConsumerID:        uuid.NewV4().String(),
		KeyDecoder:        ByteDecoder,
		ValueDecoder:      ByteDecoder,
		AutoOffsetReset:   client.EarliestTime,
		InitOffsetBackoff: 500 * time.Millisecond,
	}
}
