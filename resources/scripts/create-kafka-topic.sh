source ./vars.sh

TOPIC=$1

if [ -n "$TOPIC" ]
then
  echo "Creating the topic $TOPIC replication-factor:1, partitions:1"
  
  CMD="$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER_HOST_LIST --replication-factor 1 --partitions 1 --topic \"$TOPIC\""
  echo $CMD
  eval $CMD
  
  echo "Listing topics"
  
  CMD="$KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper $ZOOKEEPER_HOST_LIST"
  echo $CMD
  eval $CMD
else
  echo "No topic name specified"
fi  