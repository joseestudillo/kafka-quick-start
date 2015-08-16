package com.joseestudillo.kafka.old;

import java.io.IOException;
import java.util.Properties;

import com.joseestudillo.kafka.old.consumer.OldHighLevelConsumer;
import com.joseestudillo.kafka.old.producer.OldProducer;
import com.joseestudillo.kafka.old.server.ServerStarter;
import com.joseestudillo.kafka.utils.KafkaUtils;

public class LauncherOldProducerConsumer {
	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "kafka-topic";
		String zookeeper = "localhost:2181";

		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		zooKeeperDefaultConfig.setProperty("host.name", "localhost");
		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		int nBrokers = 3;

		ServerStarter server = new ServerStarter(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		server.start();

		Thread.sleep(2000); //wait for the server to be ready

		OldProducer producer = new OldProducer(server.getBrokerCSV(), topic);
		OldHighLevelConsumer consumer = new OldHighLevelConsumer(topic, "groupId", 1, zookeeper);

		new Thread(producer).start();
		new Thread(consumer).start();

		Thread.sleep(15000);

		producer.stop();
		consumer.stop();

		server.stop();

		System.exit(0);
	}
}
