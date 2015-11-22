source ./vars.sh

TOPIC=$1

if [ -n "$TOPIC" ]
then
  echo "Deleting the topic $TOPIC"
  CMD="kafka-topics.sh --delete --zookeeper $ZOOKEEPER_HOST_LIST --replication-factor 1 --partitions 1 --topic \"$TOPIC\""
  echo $CMD
  eval $CMD
  
  echo "Listing topics"
  
  CMD="kafka-topics.sh --list --zookeeper $ZOOKEEPER_HOST_LIST"
  echo $CMD
  eval $CMD
else
  echo "No topic name specified"
fi  
