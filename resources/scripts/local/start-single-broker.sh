#Start kafka
echo "Starting kafka on background..."

BROKER_DEFAULT=$KAFKA_HOME/config/server.properties

kafka-server-start.sh $BROKER_DEFAULT &
echo "done"