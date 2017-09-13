Akka Event Log
=========================

A distributed commit log, inspired by Apache Kafka. It uses Akka Cluster to manage communication between nodes. Its goal is to support an event based stream, than can be replayed as a whole or entity by entity.

How to run it
-------------------------

- To start a node, use `./start-node.sh [node_number]`. This will create an instance that listens on TCP port `7000+n`
- To produce a test record, use `./produce.sh`
- To start a consumer, use `./consume.sh`

Next steps
-------------------------

- Support replication.
- Support partitioning. Right now a topic has only one partition.
- Create configurable utilities to produce and consume.
- Create metadata for topics (right now topic information is stored using a CRDT. This means that as long as one node is up, topics metadata will be fine. But if all nodes die and only a part of them are restarted, you might lose information about some topics).
- Support multiple topics.
- Support indexing of single entities.
