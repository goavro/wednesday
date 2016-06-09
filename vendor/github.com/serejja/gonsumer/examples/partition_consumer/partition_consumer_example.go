package main

import (
	"fmt"
	"github.com/serejja/gonsumer"
	"github.com/serejja/kafka-client"
)

func main() {
	config := client.NewConfig()
	config.BrokerList = []string{"localhost:9092"}

	kafka, err := client.New(config)
	if err != nil {
		panic(err)
	}

	consumer := gonsumer.NewPartitionConsumer(kafka, gonsumer.NewConfig(), "gonsumer", 0, partitionConsumerStrategy)

	consumer.Start()
}

func partitionConsumerStrategy(data *gonsumer.FetchData, consumer *gonsumer.KafkaPartitionConsumer) {
	if data.Error != nil {
		panic(data.Error)
	}

	for _, msg := range data.Messages {
		fmt.Printf("%s from partition %d\n", string(msg.Value), msg.Partition)
	}
}
