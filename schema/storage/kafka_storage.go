package storage

import (
	"encoding/json"
	"fmt"

	"github.com/elodina/siesta"
	producer "github.com/elodina/siesta-producer"
	"github.com/yanzay/log"
)

type MessageType string

const (
	MessageSchema        MessageType = "schema"
	MessageGlobalConfig              = "global-config"
	MessageSubjectConfig             = "subject-config"
	MessageCreateUser                = "create-user"
)

type Sender interface {
	Send(*producer.ProducerRecord) <-chan *producer.RecordMetadata
}

type KafkaStorage struct {
	producer Sender
}

func NewMessageRecord(messageType MessageType, content map[string]string) (*producer.ProducerRecord, error) {
	val, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	return &producer.ProducerRecord{Topic: content["client"], Key: string(messageType), Value: val}, nil
}

func NewKafkaStorage(producer Sender) StorageWriter {
	store := &KafkaStorage{producer: producer}
	return store
}

func (ks *KafkaStorage) StoreSchema(client string, subject string, schema string) (int64, error) {
	log.Info("StoreSchema invoked")
	content := map[string]string{
		"client":  client,
		"subject": subject,
		"schema":  schema,
	}
	metadata, err := ks.send(MessageSchema, content)
	if err != nil {
		return -1, err
	}
	return metadata.Offset + 1, nil
}

func (ks *KafkaStorage) UpdateGlobalConfig(client string, config CompatibilityConfig) error {
	content := map[string]string{
		"client":        client,
		"compatibility": config.Compatibility,
	}
	_, err := ks.send(MessageGlobalConfig, content)
	return err
}

func (ks *KafkaStorage) UpdateSubjectConfig(client string, subject string, config CompatibilityConfig) error {
	content := map[string]string{
		"client":        client,
		"subject":       subject,
		"compatibility": config.Compatibility,
	}
	_, err := ks.send(MessageSubjectConfig, content)
	return err
}

func (ks *KafkaStorage) CreateUser(name string, token string, admin bool) (string, error) {
	content := map[string]string{
		"client": "admin",
		"name":   name,
		"token":  token,
		"admin":  fmt.Sprintf("%t", true),
	}
	_, err := ks.send(MessageCreateUser, content)
	return token, err
}

func (ks *KafkaStorage) send(messageType MessageType, content map[string]string) (*producer.RecordMetadata, error) {
	log.Info("Sending to Kafka")
	record, err := NewMessageRecord(messageType, content)
	if err != nil {
		return nil, err
	}
	metadata := <-ks.producer.Send(record)
	log.Infof("METADATA: %v", *metadata)
	if metadata.Error != siesta.ErrNoError {
		return nil, metadata.Error
	}
	return metadata, nil
}
