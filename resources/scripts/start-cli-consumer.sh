source ./vars.sh

TOPIC=$1

if [ -n "$TOPIC" ]
then
  CMD="kafka-console-consumer.sh --zookeeper $ZOOKEEPER_HOST_LIST --topic $TOPIC --from-beginning"
  echo $CMD
  eval $CMD
else
  echo "No topic name specified"
fi 