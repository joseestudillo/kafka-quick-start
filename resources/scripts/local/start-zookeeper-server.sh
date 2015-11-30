#Start zookeeper

echo "Starting zookeeper for kafka..."
CONFIGS_DIR=../../configs
KAFKA_ZKPR_CFG="$KAFKA_HOME/config/zookeeper.properties"
KAFKA_ZKPR_CFG="$CONFIGS_DIR/zookeeper.properties"

zookeeper-server-start.sh $KAFKA_ZKPR_CFG 
