#Start zookeeper

echo "Starting zookeeper for kafka..."

KAFKA_ZKPR_CFG="$KAFKA_HOME/config/zookeeper.properties"

zookeeper-server-start.sh $KAFKA_ZKPR_CFG 
