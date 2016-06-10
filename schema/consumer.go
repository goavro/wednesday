package schema

import (
	"encoding/json"

	"github.com/serejja/gonsumer"
	client "github.com/serejja/kafka-client"
	"github.com/yanzay/log"
	"github.com/yanzay/wednesday/schema/storage"
)

type Consumer struct {
	consumer gonsumer.Consumer
	storage  storage.StorageStateWriter
}

func NewConsumer(brokerList []string, store storage.StorageStateWriter, multiuser bool) *Consumer {
	c := &Consumer{storage: store}
	config := client.NewConfig()
	config.FetchMinBytes = 1
	config.BrokerList = brokerList
	client, err := client.New(config)
	if err != nil {
		panic(err)
	}
	consumerConfig := gonsumer.NewConfig()
	consumerConfig.Group = "wednesday-group"
	consumerConfig.AutoCommitEnable = false
	c.consumer = gonsumer.New(client, consumerConfig, c.consumerStrategy)
	if multiuser {
		c.consumer.Add("admin", 0)
	}
	return c
}

func (c *Consumer) Watch(topic string) {
	c.consumer.Add(topic, 0)
}

func (c *Consumer) Join() {
	c.consumer.Join()
}

func (c *Consumer) consumerStrategy(data *gonsumer.FetchData, _ *gonsumer.KafkaPartitionConsumer) {
	log.Info("Consumer strategy invoked")
	if data.Error != nil {
		log.Errorf("[Consumer] Fetch error: %s\n", data.Error)
	}

	for _, msg := range data.Messages {
		var err error
		log.Info(string(msg.Value))
		content := messageContent(msg.Value)
		switch storage.MessageType(msg.Key) {
		case storage.MessageSchema:
			err = c.storage.AddSchema(content["client"], content["subject"], msg.Offset+1, content["schema"])
		case storage.MessageGlobalConfig:
			err = c.storage.SetGlobalConfig(content["client"], content["compatibility"])
		case storage.MessageSubjectConfig:
			err = c.storage.SetSubjectConfig(content["client"], content["subject"], content["compatibility"])
		case storage.MessageCreateUser:
			err = c.storage.AddUser(content["name"], content["token"], content["admin"] == "true")
		default:
			log.Error("[Consumer] Unexpected message type")
		}
		if err != nil {
			log.Errorf("[Consumer] %s", err)
		}
	}
}

func messageContent(message []byte) map[string]string {
	var content map[string]string
	err := json.Unmarshal(message, &content)
	if err != nil {
		log.Fatal(err.Error())
	}
	return content
}
