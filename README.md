# Kafka

## Concepts

- Topic: A topic is a category or feed name under which messages are published. Each _partition_ is an ordered, immutable sequence of messages that is continually appended to a commit log. The messages in the partitions are each assigned a sequential id number called the _offset_ that uniquely identifies each message within the partition. A topic have the following concepts associated:

  - _leader_: is the node responsible for all reads and writes for the given partition. Each node will be the leader for a randomly selected portion of the partitions. The followers asynchronously replicate data from the leader.
  - _replicas_: is the list of nodes that replicate the log for this partition regardless of whether they are the leader or even if they are currently alive.
  - _ISR (in-sync replicas)_: is the set of "in-sync" replicas. This is the subset of the replicas list that is currently alive and caught-up to the leader. This list is stored on Zookeeper

- _Partitions_: Every partition is an ordered, immutable sequence of messages; each time a message is published to a partition, the broker appends the message to the last segment file

The partitions in the log serve several purposes. First, they allow the log to scale beyond a size that will fit on a single server. Each individual partition must fit on the servers that host it, but a topic may have many partitions so it can handle an arbitrary amount of data. Second they act as the unit of parallelism.

In a group of consumers, each partition is only consumed of only one consumer. So if there are more consumers than partitions, these will be idle.

- _Consumers_: Messaging traditionally has two models: 
  - _queuing_: a pool of consumers may read from a server and each message goes to one of them.
  - _publish-subscribe_: the message is broadcast to all consumers.
Kafka offers a single consumer abstraction that generalizes both of these _the consumer group_.

the Consumers label themselves with a consumer group name, and each message published to a topic, is delivered to only one consumer instance within each subscribing consumer group. Consumer instances can be in separate processes or on separate machines. 

If each consumer is in a different group it works in _publish-subcribe_ mode (every message go to every consumer), if there are more than one consumer per consumer group, it works in _queuing_ mode.

- Brokers:
  - Kafka broker does not maintain a record of what is consumed by whom.
  - kafka defines time-based SLA (service level agreement) as a message retention policy. In line with this policy, a message will be automatically deleted if it has been
retained in the broker longer than the defined SLA period.

## Installing kafka

Getting the binaries and declaring the env vars as follows is enough:

```bash
export KAFKA_HOME=/usr/local/apache/kafka/current
export PATH=$PATH:$KAFKA_HOME/bin
```

Where `current` is a symb link that points to the actual version of Kafka.


## Troubleshotting

- _Broker already registered_:

```
java.lang.RuntimeException: A broker is already registered on the path /brokers/ids/0. This probably indicates that you either have configured a brokerid that is already in use, or else you have shutdown this broker and restarted it faster than the zookeeper timeout so it appears to be re-registering.
```

Delete the temporary zookeeper folder. For zookeeper is defined in the property `dataDir=/tmp/zookeeper`.

## Network configuration

## CLI

To run Kafka you will need Zookepeer required by the Kafka server. The Script `resources/scripts/local/start-zookeeper-server.sh` can run a local zookepeer.

Once the Zookeeper is up, you need to register broker(s) on it. This can be done locally with the scripts:

- `resources/scripts/local/start-single-broker.sh`
- `resources/scripts/local/start-multi-broker.sh`

In the case of multiple brokers, one of them will be elected as a leader automatically. The brokers are started based on a configuration defined on a property files. By default kafka includes one in `$KAFKA_HOME/config/server.properties` to be able to create multiple in a single machine I have also added other configurations under `resources/configs/broker-x-server.properties`, basically changing the port and adding an explanation for the main parameters. These brokers, are basically the Kafka servers.

The next step is creating a topic. To create a topic you need a name for it, the number of partitions and the replication factor for them. Also for every topic you will need the list of Zookeeper host and the list of brokers. There is a command to create topics that works as follows:

```bash
kafka-topics.sh --create --zookeeper $ZOOKEEPER_HOST_LIST --replication-factor $REPLICATION_FACTOR --partitions $N_PARTITIONS --topic $TOPIC
```

The scripts `scripts/create-topic.sh` or `scripts/create-replicated-topic.sh` will do this automatically. Notice that if you don't require specific topic configuration setting the property `auto.create.topics.enable` in the brokers will create the topics automatically when they are requested.

With zookeeper up, the broker(s) running and the topic created we can now produce and consume messages. The easiest way to test this is using the console consumer and the console producer included in the Kafka binaries. To do so, the scripts  `scripts/start-cli-producer.sh` and `scripts/start-cli-consumer.sh` will do that getting the topic name as parameter. 

Apart from the scripts described above, `list-topics.sh` will show all the topics registerd for list of Zookeeper hosts and `describe-topic.sh` that will show information about a given topic, as an example, for the topic `rt0` that I have created the oput would be:

```bash
kafka-topics.sh --describe --zookeeper localhost:2181 --topic "rt0"

Topic:rt0	PartitionCount:3	ReplicationFactor:3	Configs:
	Topic: rt0	Partition: 0	Leader: 2	Replicas: 2,0,1	Isr: 2,0,1
	Topic: rt0	Partition: 1	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
	Topic: rt0	Partition: 2	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
```

## Java

### Running the examples

For most of the examples there are 2 versions, one Standalone and another that will required a zookeeper server up and the brokers subscribed. It is advisable to run a local zookeeper server and local brokers and the examples that run the server programatically are too verbose.

### Producer


To Send the messages the class `ProducerRecord` is used. There are several options showed below:

```
new ProducerRecord<String, String>(topic, msg);
new ProducerRecord<String, String>(topic, key, msg);
new ProducerRecord<String, String>(topic, partition, key, msg);
```

Besides the `topic` and the `msg`, which meaning is clear, the `key` serves as an extra layer of grouping messages and it can be null. The `partition` field allow to do the partitioning manually instead of using a partitioner. 

### Consumer

### Partitioner

As with the rest of the classes, there are different partitioner interfaces for the old and the new version of kafka (`com.joseestudillo.kafka.old.OldPartitioner` and `com.joseestudillo.kafka.NewPartitioner`). However in order to use them in a producer the property `partitioner.class` must be setup with the canonical name of the partitioner class.

## Hortonworks

## Code Examples