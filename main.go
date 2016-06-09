package main

import (
	"flag"
	"strings"

	"github.com/yanzay/log"
	"github.com/yanzay/wednesday/schema"
)

var (
	brokers      = flag.String("brokers", "localhost:9092", "Kafka broker list")
	topic        = flag.String("topic", "schemas", "Kafka topic")
	port         = flag.Int("port", 8081, "HTTP port to listen")
	cassandra    = flag.String("cassandra", "", "Cassandra nodes")
	protoVersion = flag.Int("proto-version", 3, "Cassandra protocol version")
	cqlVersion   = flag.String("cql-version", "3.0.0", "Cassandra CQL version")
)

func main() {
	flag.Parse()
	registryConfig := schema.DefaultRegistryConfig()
	registryConfig.Brokers = strings.Split(*brokers, ",")
	registryConfig.Cassandra = *cassandra
	registryConfig.Port = *port
	registryConfig.Topic = *topic
	app := schema.NewApp(registryConfig)
	err := app.Start()
	if err != nil {
		log.Fatal(err)
	}
}