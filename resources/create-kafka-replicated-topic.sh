. ./vars.sh
echo "Creating the topic $REPLICATED_TOPIC_NAME"
echo "$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER_HOST_LIST --replication-factor $REPLICATION_FACTOR --partitions $N_PARTITIONS --topic \"$REPLICATED_TOPIC_NAME\""
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER_HOST_LIST --replication-factor $REPLICATION_FACTOR --partitions $N_PARTITIONS --topic "$REPLICATED_TOPIC_NAME"

echo "Listing topics"
$KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER_HOST_LIST
