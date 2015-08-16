. ./vars.sh
echo "Creating the topic $TOPIC_NAME"
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER_HOST_LIST --replication-factor 1 --partitions 1 --topic "$TOPIC_NAME"
echo "Listing topics"
$KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER_HOST_LIST
