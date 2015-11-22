#Start kafka

CONFIGS_DIR=../../configs
BROKER_DEFAULT=$KAFKA_HOME/config/server.properties
BROKER_DEFAULT=$CONFIGS_DIR/broker-0-server.properties
BROKER_LOCAL_1=$CONFIGS_DIR/broker-1-server.properties
BROKER_LOCAL_2=$CONFIGS_DIR/broker-2-server.properties

echo "Starting kafka on background..."
CMD="
kafka-server-start.sh $BROKER_DEFAULT &
kafka-server-start.sh $BROKER_LOCAL_1 &
kafka-server-start.sh $BROKER_LOCAL_2 &
"
echo "$CMD"
eval $CMD
echo "done"


#to kill all the servers in the background for this example
#ps -ef | grep server.properties | awk '{print $2}' | xargs kill -9
