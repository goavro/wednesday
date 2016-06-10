package gonsumer

import (
	"github.com/rcrowley/go-metrics"
	"gopkg.in/stretchr/testify.v1/assert"
	"strings"
	"testing"
	"time"
)

var NoOpStrategy = func(data *FetchData, consumer *KafkaPartitionConsumer) {}

func TestConsumerAssignments(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := New(client, testConsumerConfig(), NoOpStrategy)
	consumer.(*KafkaConsumer).partitionConsumerFactory = NewMockPartitionConsumer

	// no assignments
	assignments := consumer.Assignment()
	assert.Len(t, assignments, 0)

	// add one
	err := consumer.Add("test", 0)
	assert.Equal(t, nil, err)
	assignments = consumer.Assignment()
	_, exists := assignments["test"]
	assert.True(t, exists)
	assert.Len(t, assignments["test"], 1)
	assert.Equal(t, int32(0), assignments["test"][0])

	// add existing
	err = consumer.Add("test", 0)
	assert.NotEqual(t, nil, err)
	assignments = consumer.Assignment()
	assert.Len(t, assignments, 1)

	// add another
	err = consumer.Add("test1", 1)
	assert.Equal(t, nil, err)
	assignments = consumer.Assignment()
	_, exists = assignments["test1"]
	assert.True(t, exists)
	assert.Len(t, assignments["test1"], 1)
	assert.Equal(t, int32(1), assignments["test1"][0])

	assert.Len(t, assignments, 2)

	// remove one
	err = consumer.Remove("test", 0)
	assert.Equal(t, nil, err)
	assignments = consumer.Assignment()
	assert.Len(t, assignments, 1)

	// remove non existing
	err = consumer.Remove("test", 0)
	assert.NotEqual(t, nil, err)
	assignments = consumer.Assignment()
	assert.Len(t, assignments, 1)

	// remove one that never existed
	err = consumer.Remove("asdasd", 32)
	assert.NotEqual(t, nil, err)
	assignments = consumer.Assignment()
	assert.Len(t, assignments, 1)

	// remove last
	err = consumer.Remove("test1", 1)
	assert.Equal(t, nil, err)
	assignments = consumer.Assignment()
	assert.Len(t, assignments, 0)
}

func TestConsumerOffset(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := New(client, testConsumerConfig(), NoOpStrategy)
	consumer.(*KafkaConsumer).partitionConsumerFactory = NewMockPartitionConsumer

	// offset for non-existing
	offset, err := consumer.Offset("asd", 0)
	assert.Equal(t, int64(-1), offset)
	assert.NotEqual(t, nil, err)
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Error message should contain 'does not exist' text")
	}

	// add partition consumer and ensure it has offset 0 and no error
	err = consumer.Add("test", 0)
	assert.Equal(t, nil, err)
	offset, err = consumer.Offset("test", 0)
	assert.Equal(t, int64(0), offset)
	assert.Equal(t, nil, err)

	// move offset and ensure Offset returns this value
	expectedOffset := int64(123)
	consumer.(*KafkaConsumer).partitionConsumers["test"][0].(*MockPartitionConsumer).offset = expectedOffset
	offset, err = consumer.Offset("test", 0)
	assert.Equal(t, expectedOffset, offset)
	assert.Equal(t, nil, err)

	// offset for existing topic but non-existing partition should return error
	offset, err = consumer.Offset("test", 1)
	assert.Equal(t, int64(-1), offset)
	assert.NotEqual(t, nil, err)
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Error message should contain 'does not exist' text")
	}
}

func TestConsumerCommitOffset(t *testing.T) {
	client := NewMockClient(0, 0)
	config := testConsumerConfig()
	consumer := New(client, config, NoOpStrategy)
	consumer.(*KafkaConsumer).partitionConsumerFactory = NewMockPartitionConsumer

	err := consumer.Commit("asd", 0, 123)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, client.commitCount[config.Group]["asd"][0]) //expect one commit
	assert.Equal(t, int64(123), client.offsets[config.Group]["asd"][0])
}

func TestConsumerSetOffset(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := New(client, testConsumerConfig(), NoOpStrategy)
	consumer.(*KafkaConsumer).partitionConsumerFactory = NewMockPartitionConsumer

	// set for non-existing
	err := consumer.SetOffset("asd", 0, 123)
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Error message should contain 'does not exist' text")
	}

	topic := "test"
	partition := int32(0)
	offset := int64(-1)
	seekOffset := int64(123)

	// add a topic-partition and make sure SetOffset overrides offset
	err = consumer.Add(topic, partition)
	assert.Equal(t, nil, err)
	offset, err = consumer.Offset(topic, partition)
	assert.Equal(t, int64(0), offset)
	assert.Equal(t, nil, err)

	err = consumer.SetOffset(topic, partition, seekOffset)
	assert.Equal(t, nil, err)

	offset, err = consumer.Offset(topic, partition)
	assert.Equal(t, seekOffset, offset)
	assert.Equal(t, nil, err)
}

func TestConsumerLag(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := New(client, testConsumerConfig(), NoOpStrategy)
	consumer.(*KafkaConsumer).partitionConsumerFactory = NewMockPartitionConsumer

	// lag for non-existing
	lag, err := consumer.Lag("asd", 0)
	assert.Equal(t, int64(-1), lag)
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Error message should contain 'does not exist' text")
	}

	topic := "test"
	partition := int32(0)

	err = consumer.Add(topic, partition)
	assert.Equal(t, nil, err)
	lag, err = consumer.Lag(topic, partition)
	assert.Equal(t, int64(0), lag)
	assert.Equal(t, nil, err)

	// move lag value and ensure Lag returns this value
	expectedLag := int64(123)
	consumer.(*KafkaConsumer).partitionConsumers[topic][partition].(*MockPartitionConsumer).lag = expectedLag
	lag, err = consumer.Lag(topic, partition)
	assert.Equal(t, expectedLag, lag)
	assert.Equal(t, nil, err)
}

func TestConsumerAwaitTermination(t *testing.T) {
	timeout := time.Second
	client := NewMockClient(0, 0)
	consumer := New(client, testConsumerConfig(), NoOpStrategy)
	consumer.(*KafkaConsumer).partitionConsumerFactory = NewMockPartitionConsumer

	success := make(chan struct{})
	go func() {
		consumer.AwaitTermination()
		success <- struct{}{}
	}()

	err := consumer.Add("test", 0)
	assert.Equal(t, nil, err)
	consumer.Stop()
	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Await termination failed to unblock within %s", timeout)
	}
}

func TestConsumerJoin(t *testing.T) {
	timeout := time.Second
	topic := "test"
	partition := int32(0)

	client := NewMockClient(0, 0)
	consumer := New(client, testConsumerConfig(), NoOpStrategy)
	consumer.(*KafkaConsumer).partitionConsumerFactory = NewMockPartitionConsumer

	success := make(chan struct{})

	go func() {
		consumer.Join() //should exit immediately as no topic/partitions are being consumed
		success <- struct{}{}
	}()

	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Join failed to unblock within %s", timeout)
	}

	// add one topic/partition and make sure Join does not unblock before we need it to
	err := consumer.Add(topic, partition)
	assert.Equal(t, nil, err)

	go func() {
		consumer.Join()
		success <- struct{}{}
	}()

	select {
	case <-success:
		t.Fatalf("Join unblocked while it shouldn't")
	case <-time.After(timeout):
	}

	//now remove topic-partition and make sure Join now unblocks fine
	err = consumer.Remove(topic, partition)
	assert.Equal(t, nil, err)

	select {
	case <-success:
	case <-time.After(timeout):
		t.Fatalf("Join failed to unblock within %s", timeout)
	}
}

func TestConsumerMetrics(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := New(client, NewConfig(), NoOpStrategy)

	m, err := consumer.ConsumerMetrics()
	assert.Nil(t, m)
	assert.Equal(t, ErrMetricsDisabled, err)

	consumer = New(client, testConsumerConfig(), NoOpStrategy)
	consumer.(*KafkaConsumer).partitionConsumerFactory = NewMockPartitionConsumer
	consumer.Add("foo", 0)

	m, err = consumer.ConsumerMetrics()
	assert.Nil(t, err)

	called := false
	m.NumOwnedTopicPartitions(func(numOwnedTopicPartitions metrics.Counter) {
		called = true
		assert.Equal(t, int64(1), numOwnedTopicPartitions.Count())
	})
	assert.True(t, called)
}

func TestPartitionConsumerMetrics(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := New(client, NewConfig(), NoOpStrategy)

	m, err := consumer.PartitionConsumerMetrics("foo", 0)
	assert.Nil(t, m)
	assert.Equal(t, ErrMetricsDisabled, err)

	// enable metrics but don't add topic/partition
	consumer = New(client, testConsumerConfig(), NoOpStrategy)

	m, err = consumer.PartitionConsumerMetrics("foo", 0)
	assert.Nil(t, m)
	assert.Equal(t, ErrPartitionConsumerDoesNotExist, err)

	// now add
	consumer.Add("foo", 0)

	_, err = consumer.PartitionConsumerMetrics("foo", 0)
	assert.Nil(t, err)
}

func TestAllMetrics(t *testing.T) {
	client := NewMockClient(0, 0)
	consumer := New(client, NewConfig(), NoOpStrategy)

	m, err := consumer.AllMetrics()
	assert.Nil(t, m)
	assert.Equal(t, ErrMetricsDisabled, err)

	consumer = New(client, testConsumerConfig(), NoOpStrategy)

	m, err = consumer.AllMetrics()
	assert.Nil(t, err)
	assert.NotNil(t, m.Consumer)
	assert.NotNil(t, m.PartitionConsumers)
	assert.Len(t, m.PartitionConsumers, 0)

	consumer.Add("foo", 0)

	m, err = consumer.AllMetrics()
	assert.Nil(t, err)
	assert.NotNil(t, m.Consumer)
	assert.NotNil(t, m.PartitionConsumers)
	assert.Len(t, m.PartitionConsumers, 1)
	assert.Len(t, m.PartitionConsumers["foo"], 1)
}
