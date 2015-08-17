package com.joseestudillo.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.joseestudillo.kafka.old.consumer.OldHighLevelConsumer;
import com.joseestudillo.kafka.producer.Producer;
import com.joseestudillo.kafka.utils.KafkaUtils;

public class LauncherMultiProducerConsumer {

	private static final Logger log = Logger.getLogger(LauncherMultiProducerConsumer.class);

	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "new-multi-kafka-topic";
		String zookeeper = "localhost:2181";
		String groupId = "groupId";
		int nBrokers = 4;
		int nProducers = 2;
		int nConsumers = 2;
		int threadsPerConsumer = 2;
		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		zooKeeperDefaultConfig.setProperty("host.name", "localhost");
		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");

		//ServerStarter server = new ServerStarter(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		//server.start();

		String brokersCSV = "localhost:9090,localhost:9091,localhost:9092";//server.getBrokerCSV();

		Thread.sleep(4000); //wait for the server to be ready

		log.info("------------------------CREATING ALL");
		List<Producer> producers = new ArrayList<>();
		for (int i = 0; i < nProducers; i++) {
			producers.add(new Producer(brokersCSV, topic));
		}

		List<OldHighLevelConsumer> consumers = new ArrayList<>();
		for (int i = 0; i < nConsumers; i++) {
			//Notice that if the group Id remains constant, only one of the consumers will get the message
			consumers.add(new OldHighLevelConsumer(topic, groupId + i, threadsPerConsumer, zookeeper));
		}

		log.info(String.format("STARTING ALL producers: %s consumers: %s", producers.size(), consumers.size()));
		for (Producer producer : producers) {
			new Thread(producer).start();
		}
		for (OldHighLevelConsumer consumer : consumers) {
			new Thread(consumer).start();
		}

		Thread.sleep(15000);

		log.info("STOPPING ALL");
		Logger.getRootLogger().setLevel(Level.ERROR);
		for (Producer producer : producers) {
			producer.stop();
		}
		for (OldHighLevelConsumer consumer : consumers) {
			consumer.stop();
		}

		//server.stop();

		System.exit(0);
	}
}
