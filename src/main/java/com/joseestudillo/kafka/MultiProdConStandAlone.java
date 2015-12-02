package com.joseestudillo.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.joseestudillo.kafka.consumer.NewConsumerFactory;
import com.joseestudillo.kafka.consumer.NewConsumerTask;
import com.joseestudillo.kafka.producer.NewProducerFactory;
import com.joseestudillo.kafka.producer.NewProducerTask;
import com.joseestudillo.kafka.server.KafkaServer;
import com.joseestudillo.kafka.utils.KafkaUtils;

public class MultiProdConStandAlone {

	private static final Logger log = Logger.getLogger(MultiProdConStandAlone.class);

	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "new-multi-kafka-topic";
		String groupId = "groupId";
		int nBrokers = 4;
		int nProducers = 2;
		int nConsumers = 2;

		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		KafkaServer server = new KafkaServer(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		server.start();
		String brokersCSV = server.getBrokerCSV();

		log.info(String.format("Generating %s Producer(s)", nConsumers));
		List<NewProducerTask> producers = new ArrayList<>();
		for (int i = 0; i < nProducers; i++) {
			producers.add(new NewProducerTask(NewProducerFactory.newInstance(brokersCSV), topic));
		}

		log.info(String.format("Generating %s Consumers(s)", nConsumers));
		List<NewConsumerTask> consumers = new ArrayList<>();
		for (int i = 0; i < nConsumers; i++) {
			//Notice that if the group Id remains constant, only one of the consumers will get the message
			consumers.add(new NewConsumerTask(NewConsumerFactory.newInstance(brokersCSV, groupId + i, topic)));
		}

		log.info(String.format("Starting all producers: %s consumers: %s", producers.size(), consumers.size()));
		for (NewProducerTask producer : producers) {
			new Thread(producer).start();
		}
		for (NewConsumerTask consumer : consumers) {
			new Thread(consumer).start();
		}

		Thread.sleep(15000);

		log.info("Stopping all producers and consumers");
		Logger.getRootLogger().setLevel(Level.ERROR);
		for (NewProducerTask producer : producers) {
			producer.stop();
		}
		for (NewConsumerTask consumer : consumers) {
			consumer.stop();
		}

		server.stop();

		System.exit(0);
	}
}
