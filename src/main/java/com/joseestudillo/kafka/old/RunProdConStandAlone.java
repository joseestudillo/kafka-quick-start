package com.joseestudillo.kafka.old;

import java.io.IOException;
import java.util.Properties;

import com.joseestudillo.kafka.old.consumer.OldHighLevelConsumerTask;
import com.joseestudillo.kafka.old.producer.OldProducerTask;
import com.joseestudillo.kafka.server.KafkaServer;
import com.joseestudillo.kafka.utils.KafkaUtils;

/**
 * Launch 1 kafka producer and 1 kafka consumer. This class also launch a zookeeper server and one kafka broker.
 * 
 * @author Jose Estudillo
 *
 */
public class RunProdConStandAlone {
	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "kafka-topic";
		String zookeeper = "localhost:2181";
		int nBrokers = 1;

		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		KafkaServer server = new KafkaServer(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		server.start();
		String brokersCSV = server.getBrokerCSV();

		OldProducerTask producer = new OldProducerTask(brokersCSV, topic);
		OldHighLevelConsumerTask consumer = new OldHighLevelConsumerTask(topic, "groupId", 1, zookeeper);

		new Thread(producer).start();
		new Thread(consumer).start();

		Thread.sleep(5000);

		producer.stop();
		consumer.stop();

		server.stop();

		System.err.println("DONE");
		System.exit(0);
	}
}
