source ./vars.sh

TOPIC=$1

if [ -n "$TOPIC" ]
then
  echo "Creating the topic $TOPIC with replication-factor:$REPLICATION_FACTOR, partitions:$N_PARTITIONS"
  
  CMD="$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER_HOST_LIST --replication-factor $REPLICATION_FACTOR --partitions $N_PARTITIONS --topic \"$TOPIC\""
  echo $CMD
  eval $CMD
  
  echo "Listing topics"
  $KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER_HOST_LIST
else
  echo "No topic name specified"
fi