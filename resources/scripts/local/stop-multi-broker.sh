echo "Killing Kafka Brokers..."

ps -ef | grep kafka | awk '{print $2}' | xargs kill -9
