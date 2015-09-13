package com.joseestudillo.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.joseestudillo.kafka.old.consumer.OldHighLevelConsumer;
import com.joseestudillo.kafka.old.partitioner.CustomPartitioner;
import com.joseestudillo.kafka.producer.PartitionedProducer;
import com.joseestudillo.kafka.utils.KafkaUtils;

public class LauncherMultiPartitionedProducerConsumer {

	private static final Logger log = Logger.getLogger(LauncherMultiPartitionedProducerConsumer.class);

	//TODO understand how affect increasing the number of threads per topic
	//TODO understand how partitioning works

	@SuppressWarnings(value = "all")
	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "partitioned-topic-0";
		String zookeeperURL = "localhost:2181";
		String groupId = "groupId";
		int nBrokers = 4;
		int nPartitions = 3;
		int nProducers = 2;
		int nConsumers = 3;
		int threadsPerConsumer = 1;
		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		zooKeeperDefaultConfig.setProperty("host.name", "localhost");

		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		brokerDefaultConfig.setProperty("num.partitions", String.valueOf(nPartitions));
		brokerDefaultConfig.setProperty("host", "localhost");

		// I'm having problems with the topics generated this way. It works well with the topics generated in the command line		
		//		int sessionTimeoutMs = 5000;
		//		int connectionTimeoutMs = 5000;
		//		ZkClient zkClient = new ZkClient(zookeeperURL, sessionTimeoutMs, connectionTimeoutMs);
		//		int replicationFactor = 3;
		//		Properties topicConfig = new Properties();
		//		try {
		//			log.info(String.format("Deleting %s created", topic));
		//			AdminUtils.deleteTopic(zkClient, topic);
		//		} catch (Exception e) {}
		//		topic = "new-" + System.currentTimeMillis();
		//		log.info(String.format("Creating Topic {topic: %s, nPartitions: %s, replicationFactor: %s, topicConfig: %s}", topic, nPartitions, replicationFactor,
		//				topicConfig));
		//		AdminUtils.createTopic(zkClient, topic, nPartitions, replicationFactor, topicConfig);
		//		log.info(String.format("Topic %s created", topic));

		//ServerStarter server = new ServerStarter(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		//server.start();
		String brokersCSV = "localhost:9090,localhost:9091,localhost:9092";//server.getBrokerCSV();

		Thread.sleep(4000); //wait for the server to be ready

		log.info("------------------------CREATING ALL");
		List<PartitionedProducer> producers = new ArrayList<>();
		CustomPartitioner partitioner = new CustomPartitioner(nPartitions);
		for (int i = 0; i < nProducers; i++) {
			producers.add(new PartitionedProducer(brokersCSV, topic, partitioner));
		}

		List<OldHighLevelConsumer> consumers = new ArrayList<>();
		for (int i = 0; i < nConsumers; i++) {
			//Notice that if the group Id remains constant, only one of the consumers will get the message
			consumers.add(new OldHighLevelConsumer(topic, groupId, threadsPerConsumer, zookeeperURL));
		}

		log.info(String.format("STARTING ALL producers: %s consumers: %s", producers.size(), consumers.size()));
		for (PartitionedProducer producer : producers) {
			new Thread(producer).start();
		}
		for (OldHighLevelConsumer consumer : consumers) {
			new Thread(consumer).start();
		}

		Thread.sleep(15000);

		log.info("STOPPING ALL");
		Logger.getRootLogger().setLevel(Level.ERROR);
		for (PartitionedProducer producer : producers) {
			producer.stop();
		}
		for (OldHighLevelConsumer consumer : consumers) {
			consumer.stop();
		}

		//server.stop();

		System.exit(0);
	}
}
