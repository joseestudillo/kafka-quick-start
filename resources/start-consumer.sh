. ./vars.sh
$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER_HOST_LIST --topic $TOPIC_NAME --from-beginning