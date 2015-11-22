source ./vars.sh

CMD="kafka-topics.sh --list --zookeeper $ZOOKEEPER_HOST_LIST"
echo $CMD
eval $CMD