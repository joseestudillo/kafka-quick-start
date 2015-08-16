#Start kafka
echo "Starting kafka on background..."
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
echo "done"