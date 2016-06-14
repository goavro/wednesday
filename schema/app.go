package schema

import (
	"fmt"
	"os"

	producer "github.com/elodina/siesta-producer"
	"github.com/yanzay/log"
	"github.com/yanzay/wednesday/auth"
	"github.com/yanzay/wednesday/schema/api"
	"github.com/yanzay/wednesday/schema/storage"
)

type App struct {
	store     storage.Storage
	producer  *producer.KafkaProducer
	consumer  *Consumer
	server    *api.ApiServer
	registrar string
	host      string
	port      int
}

type SchemaRegistryConfig struct {
	Multiuser    bool
	Brokers      []string
	Topic        string
	VaultURL     string
	Host         string
	Port         int
	Registrar    string
	Cassandra    string
	ProtoVersion int
	CQLVersion   string
}

func DefaultRegistryConfig() SchemaRegistryConfig {
	return SchemaRegistryConfig{
		Multiuser:    false,
		Brokers:      []string{"localhost:9092"},
		Topic:        "schemas",
		VaultURL:     "",
		Host:         "localhost",
		Port:         8081,
		Registrar:    "",
		Cassandra:    "",
		ProtoVersion: 3,
		CQLVersion:   "3.0.0",
	}
}

type MockWatcher struct{}

func (*MockWatcher) Watch(topic string) {

}

func NewApp(config SchemaRegistryConfig) *App {
	auth.InitStorage(config.VaultURL, os.Getenv("VAULT_TOKEN"))

	inmemStorage := storage.NewInMemoryStorage()

	var consumer api.Watcher
	var kafkaStorage storage.StorageWriter
	if len(config.Brokers) > 0 {
		producer := createProducer(config.Brokers)
		kafkaStorage = storage.NewKafkaStorage(producer)
		consumer = NewConsumer(config.Brokers, inmemStorage, config.Multiuser)
	} else {
		kafkaStorage = &storage.MockStorageWriter{}
		consumer = &MockWatcher{}
	}

	var store storage.Storage

	if config.Cassandra == "" {
		store = &storage.CombinedStorage{
			StorageWriter:      kafkaStorage,
			StorageStateReader: inmemStorage,
			StorageStateWriter: inmemStorage,
		}
	} else {
		cassandraStorage := storage.NewCassandraStorage(config.Cassandra, config.ProtoVersion, config.CQLVersion)
		store = &storage.CachedStorage{
			StorageWriter:      storage.NewStorageMultiwriter(kafkaStorage, cassandraStorage),
			StorageStateWriter: inmemStorage,
			Cache:              inmemStorage,
			Backend:            cassandraStorage,
		}
	}

	return &App{
		store:     store,
		server:    api.NewApiServer(fmt.Sprintf(":%d", config.Port), store, consumer, config.Multiuser, config.Topic),
		registrar: config.Registrar,
		host:      config.Host,
		port:      config.Port,
	}
}

func (a *App) Start() error {
	a.register()
	return a.server.Start()
}

func (a *App) Stop() {
	a.unregister()
}

func (a *App) register() {
	if a.registrar != "" {
		log.Info("Registering")
	}
}

func (a *App) unregister() {
	if a.registrar != "" {
		log.Info("Unregistering")
	}
}
