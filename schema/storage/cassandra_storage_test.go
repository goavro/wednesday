// +build integration

package storage

import "testing"

func prepare() *CassandraStorage {
	store := NewCassandraStorage("cassandra", 3, "3.0.0")
	err := store.connection.Query("TRUNCATE avro.schemas").Exec()
	if err != nil {
		panic(err)
	}
	return store
}

func TestNewCassandraStorage(t *testing.T) {
	store := NewCassandraStorage("cassandra", 3, "3.0.0")
	if store == nil {
		t.Fail()
	}
}

func TestCassandraAddSchema(t *testing.T) {
	store := prepare()
	store.AddSchema("snow", "testsubject", 0, testSchema)
	schema, found, err := store.GetSchemaByID("snow", 0)
	if err != nil {
		t.Fail()
	}
	if !found {
		t.Fail()
	}
	if schema != testSchema {
		t.Fail()
	}
}

func TestCassandraSetGlobalConfig(t *testing.T) {
	store := prepare()
	store.SetGlobalConfig("snow", "FULL")
	level, err := store.GetGlobalConfig("snow")
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if level != "FULL" {
		t.Fail()
	}
}

func TestCassandraSetSubjectConfig(t *testing.T) {
	store := prepare()
	store.SetSubjectConfig("snow", "testsubject", "FULL")
	level, found, err := store.GetSubjectConfig("snow", "testsubject")
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("subject not found")
		t.Fail()
	}
	if level != "FULL" {
		t.Fail()
	}
}

func TestCassandraGetSchemaByID(t *testing.T) {
	store := prepare()
	_, found, err := store.GetSchemaByID("snow", 0)
	if found {
		t.Log("Not found expected")
		t.Fail()
	}
	err = store.AddSchema("snow", "testsubject", 0, testSchema)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	_, found, _ = store.GetSchemaByID("snow", 1)
	if found {
		t.Log("Not found expected")
		t.Fail()
	}
	schema, _, _ := store.GetSchemaByID("snow", 0)
	if schema != testSchema {
		t.Logf("Schema don't match: %s != %s", schema, testSchema)
		t.Fail()
	}
}

func TestCassandraGetSubjects(t *testing.T) {
	store := prepare()
	store.AddSchema(client, "testsubject1", 0, testSchema)
	subjects, err := store.GetSubjects(client)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if len(subjects) != 1 {
		t.Logf("Expeced 1 element, got %d: %v", len(subjects), subjects)
		t.Fail()
	}
	if subjects[0] != "testsubject1" {
		t.Logf("%s != %s", subjects[0], "testsubject1")
		t.Fail()
	}
	store.AddSchema(client, "testsubject2", 1, testSchema)
	subjects, err = store.GetSubjects(client)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if len(subjects) != 2 {
		t.Logf("Expected 2 elements, got %d", len(subjects))
		t.Fail()
	}
	if subjects[1] != "testsubject1" && subjects[1] != "testsubject2" || subjects[0] != "testsubject1" && subjects[0] != "testsubject2" {
		t.Logf("Subjects mismatch: %v", subjects)
		t.Fail()
	}
}

func TestCassandraGetVersions(t *testing.T) {
	store := prepare()
	store.AddSchema(client, subject, 0, testSchema)
	_, found, _ := store.GetVersions(client, "another")
	if found {
		t.Log("not found expected")
		t.Fail()
	}
	versions, found, err := store.GetVersions(client, subject)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("subject is lost!")
		t.Fail()
	}
	if len(versions) != 1 {
		t.Logf("Expected 1 version, got %d", len(versions))
		t.Fail()
	}
	if versions[0] != 1 {
		t.Log("Expected first version")
		t.Fail()
	}
	store.AddSchema(client, subject, 1, testSchema)
	versions, found, err = store.GetVersions(client, subject)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("subject is lost!")
		t.Fail()
	}
	if len(versions) != 2 {
		t.Logf("Expected 1 version, got %d", len(versions))
		t.Fail()
	}
	if versions[0] != 1 && versions[0] != 2 || versions[1] != 1 && versions[1] != 2 {
		t.Log("Expected first version")
		t.Fail()
	}
}

func TestCassandraGetSchema(t *testing.T) {
	store := prepare()
	_, _, err := store.GetSchema(client, subject, 1)
	if err == nil {
		t.Log("Expected error")
		t.Fail()
	}
	store.AddSchema(client, subject, 1, testSchema)
	_, found, _ := store.GetSchema(client, "another", 1)
	if found {
		t.Log("Expected not found")
		t.Fail()
	}
	_, found, _ = store.GetSchema(client, subject, 2)
	if found {
		t.Log("Expected not found")
		t.Fail()
	}
	schema, found, err := store.GetSchema(client, subject, 1)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("schema not found")
		t.Fail()
	}
	if schema != testSchema {
		t.Log("schema dont match")
		t.Fail()
	}
}

func TestCassandraGetLatestSchema(t *testing.T) {
	store := prepare()
	store.AddSchema(client, subject, 0, testSchema)
	_, found, _ := store.GetLatestSchema(client, "anothersubject")
	if found {
		t.Log("not found expected")
	}
	schema, found, err := store.GetLatestSchema(client, subject)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("not found")
		t.Fail()
	}
	if schema.Schema != testSchema {
		t.Log("schema don't match")
		t.Fail()
	}
	store.AddSchema(client, subject, 1, anotherSchema)
	schema, _, _ = store.GetLatestSchema(client, subject)
	if schema.Schema != anotherSchema {
		t.Log("it's not the latest schema")
		t.Fail()
	}
	if schema.Version != 2 {
		t.Log("expected second version")
		t.Fail()
	}
}
