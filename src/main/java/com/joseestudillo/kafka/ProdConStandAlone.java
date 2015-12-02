package com.joseestudillo.kafka;

import java.io.IOException;
import java.util.Properties;

import com.joseestudillo.kafka.consumer.NewConsumerFactory;
import com.joseestudillo.kafka.consumer.NewConsumerTask;
import com.joseestudillo.kafka.producer.NewProducerFactory;
import com.joseestudillo.kafka.producer.NewProducerTask;
import com.joseestudillo.kafka.server.KafkaServer;
import com.joseestudillo.kafka.utils.KafkaUtils;

public class ProdConStandAlone {

	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "new-kafka-topic";
		int nBrokers = 3;

		Properties zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		Properties brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		KafkaServer server = new KafkaServer(zooKeeperDefaultConfig, brokerDefaultConfig, nBrokers);
		server.start();
		String brokersCSV = server.getBrokerCSV();

		Thread.sleep(4000);

		NewProducerTask producer = new NewProducerTask(NewProducerFactory.newInstance(brokersCSV), topic);
		NewConsumerTask consumer = new NewConsumerTask(NewConsumerFactory.newInstance(brokersCSV, "groupId", topic));

		new Thread(producer).start();
		new Thread(consumer).start();

		Thread.sleep(15000);

		producer.stop();
		consumer.stop();

		server.stop();

		System.exit(0);
	}
}
