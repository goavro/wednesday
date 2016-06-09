package gonsumer

import (
	"github.com/rcrowley/go-metrics"
	"github.com/serejja/kafka-client"
	"github.com/yanzay/log"
	"sync/atomic"
	"time"
)

// PartitionConsumer is an interface responsible for consuming exactly one topic/partition
// from Kafka. Used to switch between PartitionConsumer in live mode and MockPartitionConsumer in tests.
type PartitionConsumer interface {
	// Start starts consuming given topic/partition.
	Start()

	// Stop stops consuming given topic/partition.
	Stop()

	// Offset returns the last fetched offset for this partition consumer.
	Offset() int64

	// Commit commits the given offset to Kafka. Returns an error on unsuccessful commit.
	Commit(offset int64) error

	// SetOffset overrides the current fetch offset value for given topic/partition.
	// This does not commit offset but allows you to move back and forth throughout the partition.
	SetOffset(offset int64)

	// Lag returns the difference between the latest available offset in the partition and the
	// latest fetched offset by this consumer. This allows you to see how much behind the consumer is.
	Lag() int64

	// Metrics returns a metrics structure for this partition consumer. An error is returned if metrics are disabled.
	Metrics() (PartitionConsumerMetrics, error)
}

// KafkaPartitionConsumer serves to consume exactly one topic/partition from Kafka.
// This is very similar to JVM SimpleConsumer except the PartitionConsumer is able to handle
// leader changes and supports committing offsets to Kafka via kafka-client.
type KafkaPartitionConsumer struct {
	client              Client
	config              *ConsumerConfig
	topic               string
	partition           int32
	offset              int64
	highwaterMarkOffset int64
	strategy            Strategy
	metrics             PartitionConsumerMetrics
	stop                chan struct{}
}

// NewPartitionConsumer creates a new PartitionConsumer for given client and config that will
// consume given topic and partition.
// The message processing logic is passed via strategy.
func NewPartitionConsumer(client Client, config *ConsumerConfig, topic string, partition int32, strategy Strategy) PartitionConsumer {
	var metrics PartitionConsumerMetrics = noOpPartitionConsumerMetrics
	if config.EnableMetrics {
		metrics = NewKafkaPartitionConsumerMetrics(topic, partition)
	}

	return &KafkaPartitionConsumer{
		client:    client,
		config:    config,
		topic:     topic,
		partition: partition,
		strategy:  strategy,
		metrics:   metrics,
		stop:      make(chan struct{}, 1),
	}
}

// Start starts consuming a single partition from Kafka.
// This call blocks until Stop() is called.
func (pc *KafkaPartitionConsumer) Start() {
	log.Infof("Starting partition consumer for topic %s, partition %d", pc.topic, pc.partition)
	proceed := pc.initOffset()
	if !proceed {
		return
	}

	for {
		select {
		case <-pc.stop:
			{
				log.Infof("Stopping fetcher loop for topic %s, partition %d", pc.topic, pc.partition)
				return
			}
		default:
			{
				var response *client.FetchResponse
				var err error
				if pc.config.EnableMetrics {
					pc.metrics.FetchDuration(func(fetchDuration metrics.Timer) {
						fetchDuration.Time(func() {
							response, err = pc.client.Fetch(pc.topic, pc.partition, atomic.LoadInt64(&pc.offset))
						})
					})
				} else {
					response, err = pc.client.Fetch(pc.topic, pc.partition, atomic.LoadInt64(&pc.offset))
				}

				pc.metrics.NumFetches(func(numFetches metrics.Counter) {
					numFetches.Inc(1)
				})

				if err != nil {
					log.Warningf("Fetch error: %s", err)
					pc.metrics.NumFailedFetches(func(numFailedFetches metrics.Counter) {
						numFailedFetches.Inc(1)
					})
					pc.strategy(&FetchData{
						Messages: nil,
						Error:    err,
					}, pc)
					continue
				}

				data := response.Data[pc.topic][pc.partition]
				atomic.StoreInt64(&pc.highwaterMarkOffset, data.HighwaterMarkOffset)
				pc.metrics.Lag(func(lag metrics.Gauge) {
					lag.Update(pc.Lag())
				})

				if len(data.Messages) == 0 {
					pc.metrics.NumEmptyFetches(func(numEmptyFetches metrics.Counter) {
						numEmptyFetches.Inc(1)
					})
					continue
				}

				// store the offset before we actually hand off messages to user
				if len(data.Messages) > 0 {
					offsetIndex := len(data.Messages) - 1
					atomic.StoreInt64(&pc.offset, data.Messages[offsetIndex].Offset+1)
				}

				//TODO kafka-client could probably support size hints? feel like quick traversal of messages should be quicker
				// than appending to a slice if it resizes internally, should benchmark this
				var messages []*MessageAndMetadata
				collector := pc.collectorFunc(&messages)
				err = response.CollectMessages(collector)
				if err != nil {
					pc.metrics.NumFailedFetches(func(numFetches metrics.Counter) {
						numFetches.Inc(1)
					})
				}

				pc.metrics.NumFetchedMessages(func(numFetchedMessages metrics.Counter) {
					numFetchedMessages.Inc(int64(len(data.Messages)))
				})

				if pc.config.EnableMetrics {
					pc.metrics.BatchDuration(func(batchDuration metrics.Timer) {
						batchDuration.Time(func() {
							pc.strategy(&FetchData{
								Messages: messages,
								Error:    err,
							}, pc)
						})
					})
				} else {
					pc.strategy(&FetchData{
						Messages: messages,
						Error:    err,
					}, pc)
				}

				if pc.config.AutoCommitEnable && len(messages) > 0 {
					offset := messages[len(messages)-1].Offset
					err = pc.Commit(offset)
					if err != nil {
						log.Warningf("Could not commit offset %d for topic %s, partition %d", offset, pc.topic, pc.partition)
					}
				}
			}
		}
	}
}

// Stop stops consuming partition from Kafka.
// This means the PartitionConsumer will stop accepting new batches but will have a chance to finish its current work.
func (pc *KafkaPartitionConsumer) Stop() {
	log.Infof("Stopping partition consumer for topic %s, partition %d", pc.topic, pc.partition)
	pc.stop <- struct{}{}
	pc.metrics.Stop()
}

// Commit commits the given offset to Kafka. Returns an error on unsuccessful commit.
func (pc *KafkaPartitionConsumer) Commit(offset int64) error {
	pc.metrics.NumOffsetCommits(func(numOffsetCommits metrics.Counter) {
		numOffsetCommits.Inc(1)
	})
	err := pc.client.CommitOffset(pc.config.Group, pc.topic, pc.partition, offset)
	if err != nil {
		pc.metrics.NumFailedOffsetCommits(func(numFetchedMessages metrics.Counter) {
			numFetchedMessages.Inc(1)
		})
	}

	return err
}

// SetOffset overrides the current fetch offset value for given topic/partition.
// This does not commit offset but allows you to move back and forth throughout the partition.
func (pc *KafkaPartitionConsumer) SetOffset(offset int64) {
	atomic.StoreInt64(&pc.offset, offset)
}

// Offset returns the last fetched offset for this partition consumer.
func (pc *KafkaPartitionConsumer) Offset() int64 {
	return atomic.LoadInt64(&pc.offset)
}

// Lag returns the difference between the latest available offset in the partition and the
// latest fetched offset by this consumer. This allows you to see how much behind the consumer is.
func (pc *KafkaPartitionConsumer) Lag() int64 {
	return atomic.LoadInt64(&pc.highwaterMarkOffset) - atomic.LoadInt64(&pc.offset)
}

// Metrics returns a metrics structure for this partition consumer. An error is returned if metrics are disabled.
func (pc *KafkaPartitionConsumer) Metrics() (PartitionConsumerMetrics, error) {
	if !pc.config.EnableMetrics {
		return nil, ErrMetricsDisabled
	}

	return pc.metrics, nil
}

func (pc *KafkaPartitionConsumer) initOffset() bool {
	log.Infof("Initializing offset for topic %s, partition %d", pc.topic, pc.partition)
	for {
		offset, err := pc.client.GetOffset(pc.config.Group, pc.topic, pc.partition)
		if err != nil {
			if err == client.ErrUnknownTopicOrPartition {
				return pc.resetOffset()
			}
			log.Warningf("Cannot get offset for group %s, topic %s, partition %d: %s\n", pc.config.Group, pc.topic, pc.partition, err)
			select {
			case <-pc.stop:
				{
					log.Warning("PartitionConsumer told to stop trying to get offset, returning")
					return false
				}
			default:
			}
		} else {
			validOffset := offset + 1
			log.Infof("Initialized offset to %d", validOffset)
			atomic.StoreInt64(&pc.offset, validOffset)
			atomic.StoreInt64(&pc.highwaterMarkOffset, validOffset)
			return true
		}
		time.Sleep(pc.config.InitOffsetBackoff)
	}
}

func (pc *KafkaPartitionConsumer) resetOffset() bool {
	log.Infof("Resetting offset for topic %s, partition %d", pc.topic, pc.partition)
	for {
		offset, err := pc.client.GetAvailableOffset(pc.topic, pc.partition, pc.config.AutoOffsetReset)
		if err != nil {
			log.Warningf("Cannot get available offset for topic %s, partition %d: %s", pc.topic, pc.partition, err)
			select {
			case <-pc.stop:
				{
					log.Warning("PartitionConsumer told to stop trying to get offset, returning")
					return false
				}
			default:
			}
		} else {
			log.Infof("Offset reset to %d", offset)
			atomic.StoreInt64(&pc.offset, offset)
			atomic.StoreInt64(&pc.highwaterMarkOffset, offset)
			return true
		}
		time.Sleep(pc.config.InitOffsetBackoff)
	}
}

func (pc *KafkaPartitionConsumer) collectorFunc(messages *[]*MessageAndMetadata) func(topic string, partition int32, offset int64, key []byte, value []byte) error {
	return func(topic string, partition int32, offset int64, key []byte, value []byte) error {
		decodedKey, err := pc.config.KeyDecoder(key)
		if err != nil {
			log.Warning(err.Error())
			return err
		}
		decodedValue, err := pc.config.ValueDecoder(value)
		if err != nil {
			log.Warning(err.Error())
			return err
		}

		*messages = append(*messages, &MessageAndMetadata{
			Key:          key,
			Value:        value,
			Topic:        topic,
			Partition:    partition,
			Offset:       offset,
			DecodedKey:   decodedKey,
			DecodedValue: decodedValue,
		})
		return nil
	}
}

// Strategy is a function that actually processes Kafka messages.
// FetchData contains actual messages, highwater mark offset and fetch error.
// PartitionConsumer which is passed to this function allows to commit/rewind offset if necessary,
// track offset/lag, stop the consumer. Please note that you should NOT stop the consumer if using
// Consumer but rather use consumer.Remove(topic, partition) call.
// The processing happens on per-partition level - the amount of strategies running simultaneously is defined by the
// number of partitions being consumed. The next batch for topic/partition won't start until the previous one
// finishes.
type Strategy func(data *FetchData, consumer *KafkaPartitionConsumer)
