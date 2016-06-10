// +build integration

package schema

import (
	"fmt"
	"sync"
	"testing"

	avro "github.com/elodina/go-avro"
	kafro "github.com/elodina/go-kafka-avro"
	. "github.com/smartystreets/goconvey/convey"
)

func TestApp(t *testing.T) {
	Convey("Given a schema registry server", t, func() {
		app := NewApp(DefaultRegistryConfig())
		go app.Start()
		Convey("Client should work properly", func() {
			fmt.Println("Creating new client")
			client := kafro.NewCachedSchemaRegistryClient("http://localhost:8081")
			rawSchema := "{\"namespace\": \"kafka.metrics\",\"type\": \"record\",\"name\": \"Timings\",\"fields\": [{\"name\": \"id\", \"type\": \"long\"},{\"name\": \"timings\",  \"type\": {\"type\":\"array\", \"items\": \"long\"} }]}"
			schema, err := avro.ParseSchema(rawSchema)
			So(err, ShouldBeNil)
			id, err := client.Register("test1", schema)
			So(err, ShouldBeNil)
			So(id, ShouldNotEqual, 0)
		})
		SkipConvey("Load testing for race detection", func() {
			Convey("Server should accept load", func() {
				var wg = &sync.WaitGroup{}
				for i := 0; i < 100; i++ {
					wg.Add(1)
					go makeLoad(wg)
				}
				wg.Wait()
			})
		})
	})
}

func makeLoad(wg *sync.WaitGroup) {
	client := kafro.NewCachedSchemaRegistryClient("http://localhost:8081")
	rawSchema := "{\"namespace\": \"kafka.metrics\",\"type\": \"record\",\"name\": \"Timings\",\"fields\": [{\"name\": \"id\", \"type\": \"long\"},{\"name\": \"timings\",  \"type\": {\"type\":\"array\", \"items\": \"long\"} }]}"
	schema, err := avro.ParseSchema(rawSchema)
	_, err = client.Register("test1", schema)
	if err != nil {
		panic(err)
	}
	schema, err = client.GetByID(1)
	if err != nil {
		panic(err)
	}
	client.GetLatestSchemaMetadata("test1")
	client.GetVersion("test1", schema)
	wg.Done()
}
