#Start kafka
echo "Starting kafka  on background..."
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
$KAFKA_HOME/bin/kafka-server-start.sh ./broker-1-server.properties &
$KAFKA_HOME/bin/kafka-server-start.sh ./broker-2-server.properties &
echo "done"


#to kill all the servers in the background for this example
#ps -ef | grep server.properties | awk '{print $2}' | xargs kill -9