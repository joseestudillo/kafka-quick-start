package com.joseestudillo.kafka.old;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.joseestudillo.kafka.old.consumer.OldHighLevelConsumer;
import com.joseestudillo.kafka.old.partitioner.CustomPartitioner;
import com.joseestudillo.kafka.old.producer.OldPartitonedProducer;
import com.joseestudillo.kafka.utils.KafkaUtils;

import kafka.admin.AdminUtils;

public class LauncherMultiOldPartitionedProducerConsumer {

	private static final Logger log = Logger.getLogger(LauncherMultiOldPartitionedProducerConsumer.class);

	//TODO understand how affect increasing the number of threads per topic
	//TODO understand how partitioning works

	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "partitioned-kafka-topic-" + System.currentTimeMillis(); //trick to avoid problems with the deletion
		String zookeeperURL = "localhost:2181";
		String groupId = "groupId";
		int nBrokers = 4;
		int nPartitions = 5;
		int nProducers = 2;
		int nConsumers = 2;
		int threadsPerConsumer = 2;
		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		zooKeeperDefaultConfig.setProperty("host.name", "localhost");

		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		brokerDefaultConfig.setProperty("num.partitions", String.valueOf(nPartitions));
		brokerDefaultConfig.setProperty("host", "localhost");

		int sessionTimeoutMs = 5000;
		int connectionTimeoutMs = 5000;
		ZkClient zkClient = new ZkClient(zookeeperURL, sessionTimeoutMs, connectionTimeoutMs);
		int numPartitions = 8;
		int replicationFactor = 3;
		Properties topicConfig = new Properties();
		try {
			log.info(String.format("Deleting %s created", topic));
			AdminUtils.deleteTopic(zkClient, topic);
		} catch (Exception e) {}
		log.info(String.format("Creating Topic %s", topic));
		AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, topicConfig);
		log.info(String.format("Topic %s created", topic));

		//ServerStarter server = new ServerStarter(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		//server.start();
		String brokersCSV = "localhost:9090,localhost:9091,localhost:9092";//server.getBrokerCSV();

		Thread.sleep(4000); //wait for the server to be ready

		log.info("------------------------CREATING ALL");
		List<OldPartitonedProducer> producers = new ArrayList<>();
		for (int i = 0; i < nProducers; i++) {
			producers.add(new OldPartitonedProducer(brokersCSV, topic, CustomPartitioner.class));
		}

		List<OldHighLevelConsumer> consumers = new ArrayList<>();
		for (int i = 0; i < nConsumers; i++) {
			//Notice that if the group Id remains constant, only one of the consumers will get the message
			consumers.add(new OldHighLevelConsumer(topic, groupId + i, threadsPerConsumer, zookeeperURL));
		}

		log.info(String.format("STARTING ALL producers: %s consumers: %s", producers.size(), consumers.size()));
		for (OldPartitonedProducer producer : producers) {
			new Thread(producer).start();
		}
		for (OldHighLevelConsumer consumer : consumers) {
			new Thread(consumer).start();
		}

		Thread.sleep(15000);

		log.info("STOPPING ALL");
		Logger.getRootLogger().setLevel(Level.ERROR);
		for (OldPartitonedProducer producer : producers) {
			producer.stop();
		}
		for (OldHighLevelConsumer consumer : consumers) {
			consumer.stop();
		}

		//server.stop();

		System.exit(0);
	}
}
