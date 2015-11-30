#Start kafka
echo "Starting kafka on background..."

CONFIGS_DIR=../../configs
BROKER_DEFAULT=$KAFKA_HOME/config/server.properties
BROKER_DEFAULT=$CONFIGS_DIR/broker-0-server.properties

kafka-server-start.sh $BROKER_DEFAULT &
echo "done"
