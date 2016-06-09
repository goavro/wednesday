package gonsumer

import (
	"fmt"
	"github.com/rcrowley/go-metrics"
)

// PartitionConsumerMetrics is an interface for accessing and modifying PartitionConsumer metrics.
type PartitionConsumerMetrics interface {
	// BatchDuration is a timer that measures time to process a single batch of data from Kafka broker by enclosing PartitionConsumer.
	BatchDuration(func(metrics.Timer))

	// FetchDuration is a timer that measures time to fetch from Kafka broker by enclosing PartitionConsumer.
	FetchDuration(func(metrics.Timer))

	// NumFetches is a counter with a total number of fetches done by enclosing PartitionConsumer.
	NumFetches(func(metrics.Counter))

	// NumFailedFetches is a counter with a number of failed fetches done by enclosing PartitionConsumer.
	NumFailedFetches(func(metrics.Counter))

	// NumEmptyFetches is a counter with a number of fetches that returned 0 messages done by enclosing PartitionConsumer.
	NumEmptyFetches(func(metrics.Counter))

	// NumFetchedMessages is a counter with a total number of fetched messages by enclosing PartitionConsumer.
	NumFetchedMessages(func(metrics.Counter))

	// NumOffsetCommits is a counter with a total number of offset commits done by enclosing PartitionConsumer.
	NumOffsetCommits(func(metrics.Counter))

	// NumFailedOffsetCommits is a counter with a number of failed offset commits done by enclosing PartitionConsumer.
	NumFailedOffsetCommits(func(metrics.Counter))

	// Lag is a gauge with a current lag value for enclosing PartitionConsumer.
	Lag(func(metrics.Gauge))

	// Registry provides access to metrics registry for enclosing PartitionConsumer.
	Registry() metrics.Registry

	// Stop unregisters all metrics from the registry.
	Stop()
}

// KafkaPartitionConsumerMetrics implements PartitionConsumerMetrics and is used when ConsumerConfig.EnableMetrics is set to true.
type KafkaPartitionConsumerMetrics struct {
	registry metrics.Registry

	batchDuration          metrics.Timer
	fetchDuration          metrics.Timer
	numFetches             metrics.Counter
	numFailedFetches       metrics.Counter
	numEmptyFetches        metrics.Counter
	numFetchedMessages     metrics.Counter
	numOffsetCommits       metrics.Counter
	numFailedOffsetCommits metrics.Counter
	lag                    metrics.Gauge
}

// NewKafkaPartitionConsumerMetrics creates new KafkaPartitionConsumerMetrics for a given topic and partition.
func NewKafkaPartitionConsumerMetrics(topic string, partition int32) *KafkaPartitionConsumerMetrics {
	registry := metrics.NewPrefixedRegistry(fmt.Sprintf("%s.%d.", topic, partition))

	return &KafkaPartitionConsumerMetrics{
		registry:               registry,
		batchDuration:          metrics.NewRegisteredTimer("batchDuration", registry),
		fetchDuration:          metrics.NewRegisteredTimer("fetchDuration", registry),
		numFetches:             metrics.NewRegisteredCounter("numFetches", registry),
		numFailedFetches:       metrics.NewRegisteredCounter("numFailedFetches", registry),
		numEmptyFetches:        metrics.NewRegisteredCounter("numEmptyFetches", registry),
		numFetchedMessages:     metrics.NewRegisteredCounter("numFetchedMessages", registry),
		numOffsetCommits:       metrics.NewRegisteredCounter("numOffsetCommits", registry),
		numFailedOffsetCommits: metrics.NewRegisteredCounter("numFailedOffsetCommits", registry),
		lag: metrics.NewRegisteredGauge("lag", registry),
	}
}

// BatchDuration is a timer that measures time to process a single batch of data from Kafka broker by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) BatchDuration(f func(metrics.Timer)) {
	f(kpcm.batchDuration)
}

// FetchDuration is a timer that measures time to fetch from Kafka broker by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) FetchDuration(f func(metrics.Timer)) {
	f(kpcm.fetchDuration)
}

// NumFetches is a counter with a total number of fetches done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumFetches(f func(metrics.Counter)) {
	f(kpcm.numFetches)
}

// NumFailedFetches is a counter with a number of failed fetches done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumFailedFetches(f func(metrics.Counter)) {
	f(kpcm.numFailedFetches)
}

// NumEmptyFetches is a counter with a number of fetches that returned 0 messages done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumEmptyFetches(f func(metrics.Counter)) {
	f(kpcm.numEmptyFetches)
}

// NumFetchedMessages is a counter with a total number of fetched messages by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumFetchedMessages(f func(metrics.Counter)) {
	f(kpcm.numFetchedMessages)
}

// NumOffsetCommits is a counter with a total number of offset commits done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumOffsetCommits(f func(metrics.Counter)) {
	f(kpcm.numOffsetCommits)
}

// NumFailedOffsetCommits is a counter with a number of failed offset commits done by enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) NumFailedOffsetCommits(f func(metrics.Counter)) {
	f(kpcm.numFailedOffsetCommits)
}

// Lag is a gauge with a current lag value for enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) Lag(f func(metrics.Gauge)) {
	f(kpcm.lag)
}

// Registry provides access to metrics registry for enclosing PartitionConsumer.
func (kpcm *KafkaPartitionConsumerMetrics) Registry() metrics.Registry {
	return kpcm.registry
}

// Stop unregisters all metrics from the registry.
func (kpcm *KafkaPartitionConsumerMetrics) Stop() {
	kpcm.registry.UnregisterAll()
}

var noOpPartitionConsumerMetrics = new(noOpKafkaPartitionConsumerMetrics)

type noOpKafkaPartitionConsumerMetrics struct{}

func (*noOpKafkaPartitionConsumerMetrics) BatchDuration(f func(metrics.Timer))            {}
func (*noOpKafkaPartitionConsumerMetrics) FetchDuration(f func(metrics.Timer))            {}
func (*noOpKafkaPartitionConsumerMetrics) NumFetches(f func(metrics.Counter))             {}
func (*noOpKafkaPartitionConsumerMetrics) NumFailedFetches(f func(metrics.Counter))       {}
func (*noOpKafkaPartitionConsumerMetrics) NumEmptyFetches(f func(metrics.Counter))        {}
func (*noOpKafkaPartitionConsumerMetrics) NumFetchedMessages(f func(metrics.Counter))     {}
func (*noOpKafkaPartitionConsumerMetrics) NumOffsetCommits(f func(metrics.Counter))       {}
func (*noOpKafkaPartitionConsumerMetrics) NumFailedOffsetCommits(f func(metrics.Counter)) {}
func (*noOpKafkaPartitionConsumerMetrics) Lag(f func(metrics.Gauge))                      {}
func (*noOpKafkaPartitionConsumerMetrics) Registry() metrics.Registry {
	panic("Registry() call on no op metrics")
}
func (*noOpKafkaPartitionConsumerMetrics) Stop() {}
