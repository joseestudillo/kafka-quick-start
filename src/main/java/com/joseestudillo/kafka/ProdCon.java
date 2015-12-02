package com.joseestudillo.kafka;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.joseestudillo.kafka.consumer.NewConsumerFactory;
import com.joseestudillo.kafka.consumer.NewConsumerTask;
import com.joseestudillo.kafka.producer.NewProducerFactory;
import com.joseestudillo.kafka.producer.NewProducerTask;

public class ProdCon {

	private static final Logger log = Logger.getLogger(ProdCon.class);

	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = args.length > 0 ? args[0] : "new-kafka-topic";
		String brokerCSV = args.length > 1 ? args[1] : "localhost:9092";
		String groupId = args.length > 2 ? args[2] : "groupId";

		log.info(String.format("Using topic: %s, brokerCSV: %s, groupId: %s", topic, brokerCSV, groupId));

		NewProducerTask producer = new NewProducerTask(NewProducerFactory.newInstance(brokerCSV), topic);
		NewConsumerTask consumer = new NewConsumerTask(NewConsumerFactory.newInstance(brokerCSV, groupId, topic));

		new Thread(producer).start();
		new Thread(consumer).start();

		Thread.sleep(20000);

		producer.stop();
		consumer.stop();

		System.exit(0);
	}
}
