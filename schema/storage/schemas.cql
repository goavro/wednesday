CREATE KEYSPACE IF NOT EXISTS avro WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS avro.schemas (
  client varchar,
  subject varchar,
  version int,
  id int,
  avro_schema text,
  PRIMARY KEY (client, subject, version),
);

CREATE TABLE IF NOT EXISTS avro.configs (
  client varchar,
  global boolean,
  subject varchar,
  level varchar,
  PRIMARY KEY (client, global, subject),
);
