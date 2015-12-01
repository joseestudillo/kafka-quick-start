package com.joseestudillo.kafka.old;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.joseestudillo.kafka.old.consumer.OldHighLevelConsumerTask;
import com.joseestudillo.kafka.old.producer.OldProducerTask;
import com.joseestudillo.kafka.server.KafkaServer;
import com.joseestudillo.kafka.utils.KafkaUtils;

/**
 * Class to illustrate the use of several producers/consumer and the use of groups.
 * 
 * @author jo186021
 *
 */
public class OldMutiBrokerProdConStandAlone {

	private static final Logger log = Logger.getLogger(OldMutiBrokerProdConStandAlone.class);

	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "multi-kafka-topic";
		String zookeeper = "localhost:2181";
		String groupId = "groupId";
		int nBrokers = 4;
		int nProducers = 2;
		int nConsumers = 2;
		int threadsPerConsumer = 2;
		boolean partitionedTopic = false;

		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		KafkaServer server = new KafkaServer(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		server.start();
		String brokersCSV = server.getBrokerCSV();

		if (partitionedTopic) {
			topic = "partitioned-kafka-topic-" + System.currentTimeMillis(); //trick to avoid problems with the deletion
			log.info(String.format("Generating Partitioned topic: %s", topic));
			int nPartitions = 5;
			int replicationFactor = 2; //It cant be bigger than the number of brokers.
			KafkaUtils.createTopic(zookeeper, topic, nPartitions, replicationFactor);
			log.info(String.format("Partitioned topic: %s Generated", topic));
		}

		log.info(String.format("Generating %s producer(s)", nProducers));
		List<OldProducerTask> producers = new ArrayList<>();
		for (int i = 0; i < nProducers; i++) {
			producers.add(new OldProducerTask(brokersCSV, topic));
		}

		log.info(String.format("Generating %s Consumers(s)", nConsumers));
		List<OldHighLevelConsumerTask> consumers = new ArrayList<>();
		for (int i = 0; i < nConsumers; i++) {
			//Notice that if the group Id remains constant, only one of the consumers in the group will get the message.
			consumers.add(new OldHighLevelConsumerTask(topic, groupId + i, threadsPerConsumer, zookeeper));
		}

		log.info(String.format("Starting all producers: %s consumers: %s", producers.size(), consumers.size()));
		for (OldProducerTask producer : producers) {
			new Thread(producer).start();
		}
		for (OldHighLevelConsumerTask consumer : consumers) {
			new Thread(consumer).start();
		}

		Thread.sleep(15000);

		log.info("Stopping all producers and consumers");
		for (OldProducerTask producer : producers) {
			producer.stop();
		}
		for (OldHighLevelConsumerTask consumer : consumers) {
			consumer.stop();
		}

		server.stop();

		System.exit(0);
	}
}
