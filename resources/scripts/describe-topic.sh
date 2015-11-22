source ./vars.sh

TOPIC=$1

if [ -n "$TOPIC" ]
then
  CMD="kafka-topics.sh --describe --zookeeper $ZOOKEEPER_HOST_LIST --topic \"$TOPIC\""
  echo $CMD
  eval $CMD
else
  echo "No topic name specified"
fi