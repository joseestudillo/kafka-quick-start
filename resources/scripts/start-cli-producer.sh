source ./vars.sh

TOPIC=$1

if [ -n "$TOPIC" ]
then
  echo "Starting a producer for the topic $TOPIC"
  CMD="kafka-console-producer.sh --broker-list $BROKER_HOST_LIST --topic \"$TOPIC\""
  echo $CMD
  eval $CMD
else
  echo "No topic name specified"
fi 