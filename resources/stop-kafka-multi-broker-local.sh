echo "Killing Kafka Brokers..."
ps -ef | grep server.properties | awk '{print $2}' | xargs kill -9