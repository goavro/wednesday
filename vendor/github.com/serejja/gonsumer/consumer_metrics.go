package gonsumer

import (
	"fmt"
	"github.com/rcrowley/go-metrics"
)

// Metrics is a set of all metrics for one Consumer instance.
type Metrics struct {
	// Consumer is all metrics for enclosing Consumer instance.
	Consumer ConsumerMetrics

	// PartitionConsumers is a map of topic/partitions to PartitionConsumer metrics.
	PartitionConsumers map[string]map[int32]PartitionConsumerMetrics
}

// ConsumerMetrics is an interface for accessing and modifying Consumer metrics.
type ConsumerMetrics interface {
	// NumOwnedTopicPartitions is a counter which value is the number of currently owned topic partitions by
	// enclosing Consumer.
	NumOwnedTopicPartitions(func(metrics.Counter))

	// Registry provides access to metrics registry for enclosing Consumer.
	Registry() metrics.Registry

	// Stop unregisters all metrics from the registry.
	Stop()
}

// KafkaConsumerMetrics implements ConsumerMetrics and is used when ConsumerConfig.EnableMetrics is set to true.
type KafkaConsumerMetrics struct {
	registry metrics.Registry

	numOwnedTopicPartitions metrics.Counter
}

// NewKafkaConsumerMetrics creates new KafkaConsumerMetrics for a given consumer group.
func NewKafkaConsumerMetrics(groupID string, consumerID string) *KafkaConsumerMetrics {
	registry := metrics.NewPrefixedRegistry(fmt.Sprintf("%s.%s.", groupID, consumerID))

	return &KafkaConsumerMetrics{
		registry: registry,

		numOwnedTopicPartitions: metrics.NewRegisteredCounter("numOwnedTopicPartitions", registry),
	}
}

// NumOwnedTopicPartitions is a counter which value is the number of currently owned topic partitions by
// enclosing Consumer.
func (cm *KafkaConsumerMetrics) NumOwnedTopicPartitions(f func(metrics.Counter)) {
	f(cm.numOwnedTopicPartitions)
}

// Registry provides access to metrics registry for enclosing Consumer.
func (cm *KafkaConsumerMetrics) Registry() metrics.Registry {
	return cm.registry
}

// Stop unregisters all metrics from the registry.
func (cm *KafkaConsumerMetrics) Stop() {
	cm.registry.UnregisterAll()
}

var noOpConsumerMetrics = new(noOpKafkaConsumerMetrics)

type noOpKafkaConsumerMetrics struct{}

func (*noOpKafkaConsumerMetrics) NumOwnedTopicPartitions(func(metrics.Counter)) {}
func (*noOpKafkaConsumerMetrics) Registry() metrics.Registry {
	panic("Registry() call on no op metrics")
}
func (*noOpKafkaConsumerMetrics) Stop() {}
