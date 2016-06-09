package gonsumer

import "errors"

// ErrPartitionConsumerDoesNotExist is used when trying to perform any action on a topic/partition that is not
// owned by the given Consumer.
var ErrPartitionConsumerDoesNotExist = errors.New("Partition consumer does not exist")

// ErrPartitionConsumerAlreadyExists is used when trying to add a topic/partition that is already added to the
// given Consumer.
var ErrPartitionConsumerAlreadyExists = errors.New("Partition consumer already exists")

// ErrMetricsDisabled is used when trying to get consumer metrics while they are disabled.
var ErrMetricsDisabled = errors.New("Metrics are disabled. Use ConsumerConfig.EnableMetrics to enable")
