// +build integration

package storage

import (
	"fmt"
	"testing"

	"github.com/elodina/siesta"
	producer "github.com/elodina/siesta-producer"
)

var offset int64 = 1

type MockProducer struct{}

func (ms *MockProducer) Send(*producer.ProducerRecord) <-chan *producer.RecordMetadata {
	metadata := make(chan *producer.RecordMetadata, 1)
	metadata <- &producer.RecordMetadata{Offset: offset, Error: siesta.ErrNoError}
	fmt.Println("Message sent!")
	return metadata
}

func TestNewKafkaStorage(t *testing.T) {
	store := NewKafkaStorage(&MockProducer{})
	if store == nil {
		t.Log("Expected object, got nil")
		t.Fail()
	}
}

func TestStoreSchema(t *testing.T) {
	store := NewKafkaStorage(&MockProducer{})
	id, err := store.StoreSchema(client, subject, testSchema)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if id != offset+1 {
		t.Log("expected id == offset + 1")
		t.Fail()
	}
}

func TestUpdateGlobalConfig(t *testing.T) {
	store := NewKafkaStorage(&MockProducer{})
	err := store.UpdateGlobalConfig(client, CompatibilityConfig{Compatibility: "FULL"})
	if err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestUpdateSubjectConfig(t *testing.T) {
	store := NewKafkaStorage(&MockProducer{})
	err := store.UpdateSubjectConfig(client, subject, CompatibilityConfig{Compatibility: "FULL"})
	if err != nil {
		t.Log(err)
		t.Fail()
	}
}
