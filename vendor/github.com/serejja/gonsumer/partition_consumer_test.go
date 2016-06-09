package gonsumer

import (
	"fmt"
	"github.com/serejja/kafka-client"
	"github.com/yanzay/log"
	"gopkg.in/stretchr/testify.v1/assert"
	"testing"
	"time"
)

type MockPartitionConsumer struct {
	offset int64
	lag    int64
}

func NewMockPartitionConsumer(client Client, config *ConsumerConfig, topic string, partition int32, strategy Strategy) PartitionConsumer {
	return new(MockPartitionConsumer)
}

func (mpc *MockPartitionConsumer) Start() {
	log.Debugf("MockPartitionConsumer.Start()")
}

func (mpc *MockPartitionConsumer) Stop() {
	log.Debugf("MockPartitionConsumer.Stop()")
}

func (mpc *MockPartitionConsumer) Offset() int64 {
	log.Debugf("MockPartitionConsumer.Offset()")
	return mpc.offset
}

func (mpc *MockPartitionConsumer) Commit(offset int64) error {
	log.Debugf("MockPartitionConsumer.Commit()")
	return nil
}

func (mpc *MockPartitionConsumer) SetOffset(offset int64) {
	log.Debugf("MockPartitionConsumer.SetOffset()")
	mpc.offset = offset
}

func (mpc *MockPartitionConsumer) Lag() int64 {
	log.Debugf("MockPartitionConsumer.Lag()")
	return mpc.lag
}

func (mpc *MockPartitionConsumer) Metrics() (PartitionConsumerMetrics, error) {
	log.Debugf("MockPartitionConsumer.Metrics()")
	return nil, nil
}

func TestPartitionConsumerSingleFetch(t *testing.T) {
	topic := "test"
	partition := int32(0)

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for i, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", i), string(msg.Value))
			assert.Equal(t, topic, msg.Topic)
			assert.Equal(t, partition, msg.Partition)
			assert.Equal(t, int64(i), msg.Offset)
		}

		assert.Len(t, data.Messages, 100)
		consumer.Stop()
	}

	client := NewMockClient(0, 100)
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerMultipleFetchesFromStart(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 512
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
			assert.Equal(t, topic, msg.Topic)
			assert.Equal(t, partition, msg.Partition)
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerMultipleFetches(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 1624
	expectedMessages := 512
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
			assert.Equal(t, topic, msg.Topic)
			assert.Equal(t, partition, msg.Partition)
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(int64(startOffset), int64(startOffset+expectedMessages))
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerEmptyFetch(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 512
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
			assert.Equal(t, topic, msg.Topic)
			assert.Equal(t, partition, msg.Partition)
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	client := NewMockClient(0, int64(expectedMessages))
	client.emptyFetches = 2
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerFetchError(t *testing.T) {
	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, client.ErrEOF, data.Error)

		consumer.Stop()
	}

	kafkaClient := NewMockClient(0, 200)
	kafkaClient.fetchError = client.ErrEOF
	kafkaClient.fetchErrorTimes = 1
	consumer := NewPartitionConsumer(kafkaClient, testConsumerConfig(), "test", 0, strategy)

	consumer.Start()
}

func TestPartitionConsumerFetchResponseError(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	kafkaClient := NewMockClient(0, int64(expectedMessages))
	kafkaClient.fetchError = client.ErrUnknownTopicOrPartition
	consumer := NewPartitionConsumer(kafkaClient, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerGetOffsetErrors(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200
	actualMessages := 0

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		for _, msg := range data.Messages {
			assert.Equal(t, fmt.Sprintf("message-%d", msg.Offset), string(msg.Value))
		}

		actualMessages += len(data.Messages)

		if actualMessages == expectedMessages {
			consumer.Stop()
		}
	}

	kafkaClient := NewMockClient(0, int64(expectedMessages))
	kafkaClient.getOffsetError = client.ErrUnknownTopicOrPartition
	kafkaClient.getOffsetErrorTimes = 2
	kafkaClient.getAvailableOffsetError = client.ErrEOF
	kafkaClient.getAvailableOffsetErrorTimes = 2
	consumer := NewPartitionConsumer(kafkaClient, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerStopOnInitOffset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		t.Fatal("Should not reach here")
	}

	kafkaClient := NewMockClient(0, int64(expectedMessages))
	kafkaClient.getOffsetError = client.ErrUnknownTopicOrPartition
	kafkaClient.getOffsetErrorTimes = 3
	kafkaClient.getAvailableOffsetError = client.ErrEOF
	kafkaClient.getAvailableOffsetErrorTimes = 3
	consumer := NewPartitionConsumer(kafkaClient, testConsumerConfig(), topic, partition, strategy)

	go func() {
		time.Sleep(1 * time.Second)
		consumer.Stop()
	}()
	consumer.Start()
}

func TestPartitionConsumerStopOnOffsetReset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	expectedMessages := 200

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		t.Fatal("Should not reach here")
	}

	kafkaClient := NewMockClient(0, int64(expectedMessages))
	kafkaClient.getOffsetError = client.ErrNotCoordinatorForConsumerCode
	kafkaClient.getOffsetErrorTimes = 3
	kafkaClient.getAvailableOffsetError = client.ErrEOF
	kafkaClient.getAvailableOffsetErrorTimes = 3
	consumer := NewPartitionConsumer(kafkaClient, testConsumerConfig(), topic, partition, strategy)

	go func() {
		time.Sleep(1 * time.Second)
		consumer.Stop()
	}()
	consumer.Start()
}

func TestPartitionConsumerOffsetAndLag(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 134
	highwaterMarkOffset := 17236

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)
		assert.Equal(t, int64(startOffset+len(data.Messages)), consumer.Offset())
		assert.Equal(t, int64(highwaterMarkOffset-(startOffset+len(data.Messages))), consumer.Lag())

		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), int64(highwaterMarkOffset))
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerSetOffset(t *testing.T) {
	topic := "test"
	partition := int32(0)
	startOffset := 134
	setOffsetDone := false

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		assert.Equal(t, int64(startOffset), data.Messages[0].Offset)

		if setOffsetDone {
			consumer.Stop()
			return
		}

		consumer.SetOffset(int64(startOffset))
		setOffsetDone = true
	}

	client := NewMockClient(int64(startOffset), int64(startOffset+100))
	consumer := NewPartitionConsumer(client, testConsumerConfig(), topic, partition, strategy)

	consumer.Start()
}

func TestPartitionConsumerCommit(t *testing.T) {
	config := testConsumerConfig()
	topic := "test"
	partition := int32(0)
	startOffset := 134
	hwOffset := int64(startOffset + 100)

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		consumer.Commit(data.Messages[len(data.Messages)-1].Offset)
		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), hwOffset)
	consumer := NewPartitionConsumer(client, config, topic, partition, strategy)
	client.initOffsets(config.Group, topic, partition)
	assert.Equal(t, int64(0), client.offsets[config.Group][topic][partition])

	consumer.Start()
	assert.Equal(t, hwOffset-1, client.offsets[config.Group][topic][partition])

	assert.Equal(t, 1, client.commitCount[config.Group][topic][partition])
}

func TestPartitionConsumerAutoCommit(t *testing.T) {
	config := testConsumerConfig()
	config.AutoCommitEnable = true
	topic := "test"
	partition := int32(0)
	startOffset := 134
	hwOffset := int64(startOffset + 100)

	strategy := func(data *FetchData, consumer *KafkaPartitionConsumer) {
		assert.Equal(t, nil, data.Error)

		consumer.Stop()
	}

	client := NewMockClient(int64(startOffset), hwOffset)
	consumer := NewPartitionConsumer(client, config, topic, partition, strategy)
	client.initOffsets(config.Group, topic, partition)
	assert.Equal(t, int64(0), client.offsets[config.Group][topic][partition])

	consumer.Start()
	assert.Equal(t, hwOffset-1, client.offsets[config.Group][topic][partition])

	assert.Equal(t, 1, client.commitCount[config.Group][topic][partition])
}

func testConsumerConfig() *ConsumerConfig {
	config := NewConfig()
	config.EnableMetrics = true
	return config
}
