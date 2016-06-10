# wednesday
Fast Avro Schema Registry

![Wednesday](https://upload.wikimedia.org/wikipedia/en/a/ab/Wednesdayswim.jpg)

This system is under active development and **not production ready**.

Wednesday is an Avro Schema Registry fully API-compatible with [Confluent Schema Registry](http://docs.confluent.io/2.0.0/schema-registry/docs/index.html).

Additional features:
- schemas stored in Cassanrda
- high-available cluster mode
- authentication support

# Guide

- Modes
  - [Standalone mode](#standalone-mode)
  - [Cluster mode](#cluster-mode)
- Storage
  - [In-memory](#in-memory-storage)
  - [Cassandra](#cassandra-storage)
- Authentication
  - [In-memory](#in-memory)
  - [Cassandra](#cassandra)
  - [Vault](#vault)

# Installation

```
$ go get -u github.com/yanzay/wednesday
```

# Usage

```
  -brokers string
      Kafka broker list (default "localhost:9092")
  -cassandra string
      Cassandra nodes
  -cql-version string
      Cassandra CQL version (default "3.0.0")
  -log-level value
      Log level: trace|debug|info|warning|error|fatal (default info)
  -port int
      HTTP port to listen (default 8081)
  -proto-version int
      Cassandra protocol version (default 3)
  -topic string
      Kafka topic (default "schemas")
```

# Modes

## Standalone mode

You can launch wednesday in standalone mode, just don't set `--brokers` parameter.
```
$ wednesday
```

## Cluster mode

Apache Kafka is used to sync state between instances for cluster mode.
So you should launch Kafka and give wednesday broker list via `--brokers` parameter.
```
$ wednesday --brokers "broker1:9092,broker2:9092,broker3:9092"
```

# Storage

## In-memory storage

By default wednesday uses in-memory storage to store schemas.
It is synced via Kafka and if you relaunch instance,
it will first sync all available schemas from Kafka.

## Cassandra storage

You can use Cassandra to store schemas and configs.
Just set `--cassandra` parameter pointing to Cassandra cluster.
Optionally you can set `--cql-version` and `--proto-version`.

```
$ wednesday --cassandra "cassandra1.cluster,cassandra2.cluster"
```

# Authentication

TODO

## In-memory

TODO

## Cassandra

TODO

## Vault

TODO
