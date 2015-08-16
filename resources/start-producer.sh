. ./vars.sh
echo "Starting a producer for the topic $TOPIC_NAME"
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $BROKER_HOST_LIST --topic "$TOPIC_NAME"