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

	consumer := gonsumer.New(kafka, gonsumer.NewConfig(), consumerStrategy)
	consumer.Add("gonsumer", 0)
	consumer.Add("gonsumer", 1)

	consumer.Join()
}

func consumerStrategy(data *gonsumer.FetchData, consumer *gonsumer.KafkaPartitionConsumer) {
	if data.Error != nil {
		panic(data.Error)
	}

	for _, msg := range data.Messages {
		fmt.Printf("%s from partition %d\n", string(msg.Value), msg.Partition)
	}
}
