package storage

import (
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/yanzay/log"
)

const maxRetries = 15

type CassandraStorage struct {
	connection *gocql.Session
}

func NewCassandraStorage(urls string, protoVersion int, cqlVersion string) *CassandraStorage {
	nodes := strings.Split(urls, ",")
	cluster := gocql.NewCluster(nodes...)
	cluster.CQLVersion = cqlVersion
	cluster.ProtoVersion = protoVersion
	cluster.ReconnectInterval = 5 * time.Second
	// cluster.Keyspace = "avro"
	var err error
	var session *gocql.Session
	retries := 0
	for session, err = cluster.CreateSession(); err != nil && retries < maxRetries; session, err = cluster.CreateSession() {
		log.Infof("Can't connect to cassandra: %s", err)
		log.Info("Retrying...")
		time.Sleep(3 * time.Second)
		retries++
	}
	if err != nil {
		log.Fatal(err)
	}
	err = initStorage(session)
	if err != nil {
		log.Fatal(err)
	}
	return &CassandraStorage{
		connection: session,
	}
}

// implement StorageStateReader interface
func (cs *CassandraStorage) Empty() bool {
	var count *int
	err := cs.connection.Query("SELECT COUNT(*) FROM avro.schemas;").Scan(&count)
	if err != nil {
		log.Error(err)
		return false
	}
	return *count == 0
}

func (cs *CassandraStorage) GetID(client string, schema string) int64 {
	iter := cs.connection.Query("SELECT id, avro_schema FROM avro.schemas WHERE client = ?", client).Iter()
	var id *int64
	var storedSchema *string
	for iter.Scan(&id, &storedSchema) {
		if schema == *storedSchema {
			return *id
		}
	}
	return -1
}

func (cs *CassandraStorage) GetSchemaByID(client string, id int64) (string, bool, error) {
	iter := cs.connection.Query("SELECT id, avro_schema FROM avro.schemas WHERE client = ?", client).Iter()
	var storedId *int64
	var schema *string
	for iter.Scan(&storedId, &schema) {
		if id == *storedId {
			return *schema, true, nil
		}
	}
	if err := iter.Close(); err != nil {
		return "", false, err
	}
	return "", false, nil
}

func (cs *CassandraStorage) GetSubjects(client string) ([]string, error) {
	iter := cs.connection.Query("SELECT subject FROM avro.schemas WHERE client = ?", client).Iter()
	subjects := make([]string, 0)
	var subject *string
	for iter.Scan(&subject) {
		subjects = append(subjects, *subject)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return subjects, nil
}

func (cs *CassandraStorage) GetVersions(client string, subject string) ([]int, bool, error) {
	iter := cs.connection.Query("SELECT version FROM avro.schemas WHERE client = ? AND subject = ?", client, subject).Iter()
	versions := make([]int, 0)
	var version *int
	for iter.Scan(&version) {
		versions = append(versions, *version)
	}
	if err := iter.Close(); err != nil {
		return nil, false, err
	}
	return versions, len(versions) > 0, nil
}

func (cs *CassandraStorage) GetSchema(client string, subject string, version int) (string, bool, error) {
	var schema *string
	err := cs.connection.Query("SELECT avro_schema FROM avro.schemas WHERE client = ? AND subject = ? AND version = ?",
		client, subject, version).Consistency(gocql.One).Scan(&schema)
	if err != nil {
		return "", false, err
	}
	return *schema, *schema != "", nil
}

func (cs *CassandraStorage) GetLatestSchema(client string, subject string) (*Schema, bool, error) {
	latest := &Schema{Subject: subject}
	found := false
	iter := cs.connection.Query("SELECT id, avro_schema, version FROM avro.schemas WHERE client = ? AND subject = ?", client, subject).Iter()
	var id *int64
	var schema *string
	var version *int
	for iter.Scan(&id, &schema, &version) {
		found = true
		if *version > latest.Version {
			latest.ID = *id
			latest.Schema = *schema
			latest.Version = *version
		}
	}
	if err := iter.Close(); err != nil {
		return nil, false, err
	}
	return latest, found, nil
}

func (cs *CassandraStorage) GetGlobalConfig(client string) (string, error) {
	var level *string
	err := cs.connection.Query("SELECT level FROM avro.configs WHERE client = ? AND global = true",
		client).Consistency(gocql.One).Scan(&level)
	if err != nil {
		return "", err
	}
	return *level, nil
}

func (cs *CassandraStorage) GetSubjectConfig(client string, subject string) (string, bool, error) {
	var level string
	err := cs.connection.Query("SELECT level FROM avro.configs WHERE client = ? AND global = false AND subject = ?",
		client, subject).Consistency(gocql.One).Scan(&level)
	if err != nil {
		return "", false, err
	}
	return level, true, nil
}

func (cs *CassandraStorage) UserByName(name string) (*User, bool) {
	return nil, false
}

func (cs *CassandraStorage) UserByToken(token string) (*User, bool) {
	return nil, false
}

// implement StorageStateWriter interface
func (cs *CassandraStorage) AddSchema(client string, subject string, id int64, schema string) error {
	versions, found, err := cs.GetVersions(client, subject)
	if err != nil {
		return err
	}
	newVersion := 1
	if found {
		for _, version := range versions {
			if newVersion <= version {
				newVersion = version + 1
			}
		}
	}
	return cs.connection.Query("INSERT INTO avro.schemas (client, subject, version, id, avro_schema) VALUES (?, ?, ?, ?, ?)",
		client, subject, newVersion, id, schema).Exec()
}

func (cs *CassandraStorage) SetGlobalConfig(client string, level string) error {
	return cs.connection.Query("INSERT INTO avro.configs (client, global, subject, level) VALUES (?, true, '', ?)", client, level).Exec()
}

func (cs *CassandraStorage) SetSubjectConfig(client string, subject string, level string) error {
	return cs.connection.Query("INSERT INTO avro.configs (client, global, subject, level) VALUES (?, false, ?, ?)", client, subject, level).Exec()
}

func (cs *CassandraStorage) AddUser(name string, token string, admin bool) error {
	return nil
}

func initStorage(session *gocql.Session) error {
	createKeyspace := `
CREATE KEYSPACE IF NOT EXISTS avro WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
`
	createSchemas := `CREATE TABLE IF NOT EXISTS avro.schemas (
  client varchar,
  subject varchar,
  version int,
  id int,
  avro_schema text,
  PRIMARY KEY (client, subject, version),
);
`
	createConfigs := `CREATE TABLE IF NOT EXISTS avro.configs (
  client varchar,
  global boolean,
  subject varchar,
  level varchar,
  PRIMARY KEY (client, global, subject),
);
	`
	err := session.Query(createKeyspace).Exec()
	if err != nil {
		return err
	}
	err = session.Query(createSchemas).Exec()
	if err != nil {
		return err
	}
	return session.Query(createConfigs).Exec()
}
