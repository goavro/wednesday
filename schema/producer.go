package schema

import (
	"github.com/elodina/siesta"
	producer "github.com/elodina/siesta-producer"
	"github.com/yanzay/log"
)

func createProducer(brokerList []string) *producer.KafkaProducer {
	connector := createConnector(brokerList)
	producerConfig := producer.NewProducerConfig()
	producerConfig.BatchSize = 1
	producerConfig.ClientID = "wednesday"
	producerConfig.SendRoutines = 2
	producerConfig.ReceiveRoutines = 2
	return producer.NewKafkaProducer(producerConfig, producer.StringSerializer, producer.ByteSerializer, connector)
}

func createConnector(brokerList []string) *siesta.DefaultConnector {
	config := siesta.NewConnectorConfig()
	config.BrokerList = brokerList

	connector, err := siesta.NewDefaultConnector(config)
	if err != nil {
		log.Fatal(err.Error())
	}
	return connector
}
