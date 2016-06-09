package gonsumer

import (
	"fmt"
	"github.com/serejja/kafka-client"
	"github.com/yanzay/log"
	"gopkg.in/stretchr/testify.v1/assert"
	"testing"
)

type MockClient struct {
	fetchSize          int64
	fetchError         error
	fetchErrorTimes    int
	fetchResponseError error
	emptyFetches       int

	getOffsetError      error
	getOffsetErrorTimes int
	offset              int64
	highwaterMarkOffset int64

	getAvailableOffsetError      error
	getAvailableOffsetErrorTimes int
	availableOffset              int64

	offsets     map[string]map[string]map[int32]int64
	commitCount map[string]map[string]map[int32]int
}

func NewMockClient(startOffset int64, highwaterMarkOffset int64) *MockClient {
	return &MockClient{
		fetchSize:           100,
		fetchResponseError:  client.ErrNoError,
		offset:              startOffset,
		highwaterMarkOffset: highwaterMarkOffset,
		offsets:             make(map[string]map[string]map[int32]int64),
		commitCount:         make(map[string]map[string]map[int32]int),
	}
}

func (mc *MockClient) Fetch(topic string, partition int32, offset int64) (*client.FetchResponse, error) {
	log.Debugf("MockClient.Fetch(%s, %d, %d)", topic, partition, offset)
	mc.offset = offset
	if mc.fetchErrorTimes > 0 {
		log.Debugf("MockClient.Fetch() should return error %d more times", mc.fetchErrorTimes)
		mc.fetchErrorTimes--
		return nil, mc.fetchError
	}

	responseData := make(map[string]map[int32]*client.FetchResponsePartitionData)
	responseData[topic] = make(map[int32]*client.FetchResponsePartitionData)
	responseData[topic][partition] = &client.FetchResponsePartitionData{
		Error:               mc.fetchResponseError,
		HighwaterMarkOffset: mc.highwaterMarkOffset,
	}

	var messages []*client.MessageAndOffset
	for i := 0; i < int(mc.fetchSize); i++ {
		if mc.emptyFetches > 0 {
			mc.emptyFetches--
			break
		}
		if mc.offset == mc.highwaterMarkOffset {
			break
		}
		messages = append(messages, &client.MessageAndOffset{
			Offset: offset + int64(i),
			Message: &client.Message{
				Value: []byte(fmt.Sprintf("message-%d", offset+int64(i))),
			},
		})
		mc.offset++
	}

	responseData[topic][partition].Messages = messages

	return &client.FetchResponse{
		Data: responseData,
	}, nil
}

func (mc *MockClient) GetOffset(group string, topic string, partition int32) (int64, error) {
	log.Debugf("MockClient.GetOffset(%s, %s, %d)", group, topic, partition)
	if mc.getOffsetErrorTimes > 0 {
		log.Debugf("MockClient.GetOffset() should return error %d more times", mc.getOffsetErrorTimes)
		mc.getOffsetErrorTimes--
		return -1, mc.getOffsetError
	}
	return mc.offset - 1, nil
}

func (mc *MockClient) CommitOffset(group string, topic string, partition int32, offset int64) error {
	mc.initOffsets(group, topic, partition)
	mc.initCommitCounts(group, topic, partition)
	mc.offsets[group][topic][partition] = offset
	mc.commitCount[group][topic][partition] = mc.commitCount[group][topic][partition] + 1

	return nil
}

func (mc *MockClient) GetAvailableOffset(topic string, partition int32, offsetTime int64) (int64, error) {
	log.Debugf("MockClient.GetAvailableOffset(%s, %d, %d)", topic, partition, offsetTime)
	if mc.getAvailableOffsetErrorTimes > 0 {
		log.Debugf("MockClient.GetAvailableOffset() should return error %d more times", mc.getAvailableOffsetErrorTimes)
		mc.getAvailableOffsetErrorTimes--
		return -1, mc.getAvailableOffsetError
	}
	return mc.availableOffset, nil
}

func (mc *MockClient) initOffsets(group string, topic string, partition int32) {
	if mc.offsets[group] == nil {
		mc.offsets[group] = make(map[string]map[int32]int64)
	}

	if mc.offsets[group][topic] == nil {
		mc.offsets[group][topic] = make(map[int32]int64)
	}
}

func (mc *MockClient) initCommitCounts(group string, topic string, partition int32) {
	if mc.commitCount[group] == nil {
		mc.commitCount[group] = make(map[string]map[int32]int)
	}

	if mc.commitCount[group][topic] == nil {
		mc.commitCount[group][topic] = make(map[int32]int)
	}
}

func TestMockClientGoodFetch(t *testing.T) {
	kafkaClient := NewMockClient(0, 200)
	response, err := kafkaClient.Fetch("test", 0, 0)
	assert.Equal(t, nil, err)
	assert.Len(t, response.Data, 1)
	assert.Len(t, response.Data["test"], 1)
	assert.Equal(t, client.ErrNoError, response.Data["test"][0].Error)
	assert.Len(t, response.Data["test"][0].Messages, int(kafkaClient.fetchSize))
	assert.Equal(t, kafkaClient.highwaterMarkOffset, response.Data["test"][0].HighwaterMarkOffset)

	for i := 0; i < 100; i++ {
		assert.Equal(t, int64(i), response.Data["test"][0].Messages[i].Offset)
		assert.Equal(t, fmt.Sprintf("message-%d", i), string(response.Data["test"][0].Messages[i].Message.Value))
	}
}

func TestMockClientBadFetch(t *testing.T) {
	kafkaClient := NewMockClient(0, 100)
	kafkaClient.fetchResponseError = client.ErrBrokerNotAvailable
	response, err := kafkaClient.Fetch("test", 0, 0)
	assert.Equal(t, nil, err)
	assert.Len(t, response.Data, 1)
	assert.Len(t, response.Data["test"], 1)
	assert.Equal(t, client.ErrBrokerNotAvailable, response.Data["test"][0].Error)
}
