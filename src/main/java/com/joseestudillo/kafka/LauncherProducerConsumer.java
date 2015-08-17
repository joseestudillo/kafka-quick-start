package com.joseestudillo.kafka;

import java.io.IOException;
import java.util.Properties;

import com.joseestudillo.kafka.old.consumer.OldHighLevelConsumer;
import com.joseestudillo.kafka.producer.Producer;
import com.joseestudillo.kafka.utils.KafkaUtils;

public class LauncherProducerConsumer {
	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "new-kafka-topic";
		String zookeeper = "localhost:2181";

		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		zooKeeperDefaultConfig.setProperty("host.name", "localhost");
		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		int nBrokers = 3;

		//ServerStarter server = new ServerStarter(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		//server.start();

		Thread.sleep(2000); //wait for the server to be ready

		String brokersCSV = "localhost:9090,localhost:9091,localhost:9092";//server.getBrokerCSV();
		Producer producer = new Producer(brokersCSV, topic);
		OldHighLevelConsumer consumer = new OldHighLevelConsumer(topic, "groupId", 1, zookeeper);

		new Thread(producer).start();
		new Thread(consumer).start();

		Thread.sleep(15000);

		producer.stop();
		consumer.stop();

		//server.stop();

		System.exit(0);
	}
}
