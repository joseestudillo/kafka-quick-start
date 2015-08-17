. ./vars.sh
echo "Creating the topic $1"
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER_HOST_LIST --replication-factor 1 --partitions 1 --topic "$1"
echo "Listing topics"
$KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER_HOST_LIST
