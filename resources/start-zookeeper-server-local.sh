#Start zookeeper
echo "Starting zookeeper for kafka..."
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
