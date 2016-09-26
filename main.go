package main

import (
	"flag"
	"strings"

	"github.com/goavro/wednesday/schema"
	"github.com/yanzay/log"
)

var (
	brokers      = flag.String("brokers", "", "Kafka broker list")
	topic        = flag.String("topic", "schemas", "Kafka topic")
	port         = flag.Int("port", 8081, "HTTP port to listen")
	cassandra    = flag.String("cassandra", "", "Cassandra nodes")
	protoVersion = flag.Int("proto-version", 3, "Cassandra protocol version")
	cqlVersion   = flag.String("cql-version", "3.0.0", "Cassandra CQL version")
)

func main() {
	flag.Parse()
	registryConfig := schema.DefaultRegistryConfig()
	if len(*brokers) > 0 {
		registryConfig.Brokers = strings.Split(*brokers, ",")
	} else {
		registryConfig.Brokers = []string{}
	}

	registryConfig.Cassandra = *cassandra
	registryConfig.Port = *port
	registryConfig.Topic = *topic
	app := schema.NewApp(registryConfig)
	err := app.Start()
	if err != nil {
		log.Fatal(err)
	}
}
