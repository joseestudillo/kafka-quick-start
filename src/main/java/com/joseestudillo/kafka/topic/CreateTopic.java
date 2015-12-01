package com.joseestudillo.kafka.topic;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.joseestudillo.kafka.server.KafkaServer;
import com.joseestudillo.kafka.utils.KafkaUtils;

public class CreateTopic {

	private static final Logger log = Logger.getLogger(CreateTopic.class);

	public static void main(String[] args) throws IOException, InterruptedException {
		String zookeeper = "localhost:2181";
		String topic = "partitioned-kafka-topic-" + System.currentTimeMillis(); //trick to avoid problems with the deletion
		log.info(String.format("Generating Partitioned topic: %s", topic));
		int nPartitions = 5;
		int replicationFactor = 2; //It cant be bigger than the number of brokers.
		int nBrokers = 2;

		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		KafkaServer server = new KafkaServer(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		server.start();

		TopicUtils.createTopic(zookeeper, topic, nPartitions, replicationFactor);
		log.info(String.format("Partitioned topic: %s Generated", topic));

		server.stop();

		System.exit(0);
	}
}
