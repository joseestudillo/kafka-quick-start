# Kafka

## Concepts

- Topic: A topic is a category or feed name under which messages are published. Each _partition_ is an ordered, immutable sequence of messages that is continually appended to a commit log. The messages in the partitions are each assigned a sequential id number called the _offset_ that uniquely identifies each message within the partition. A topic have the following concepts associated:

  - _leader_: is the node responsible for all reads and writes for the given partition. Each node will be the leader for a randomly selected portion of the partitions. The followers asynchronously replicate data from the leader.
  - _replicas_: is the list of nodes that replicate the log for this partition regardless of whether they are the leader or even if they are currently alive.
  - _ISR (in-sync replicas)_: is the set of "in-sync" replicas. This is the subset of the replicas list that is currently alive and caught-up to the leader. This list is stored on Zookeeper

- _Partitions_: Every partition is an ordered, immutable sequence of messages; each time a message is published to a partition, the broker appends the message to the last segment file

The partitions in the log serve several purposes. First, they allow the log to scale beyond a size that will fit on a single server. Each individual partition must fit on the servers that host it, but a topic may have many partitions so it can handle an arbitrary amount of data. Second they act as the unit of parallelism.

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

## Running Kafka

## Code Examples