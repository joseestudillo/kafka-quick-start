package com.joseestudillo.kafka;

import java.io.IOException;

import com.joseestudillo.kafka.consumer.NewConsumerTask;
import com.joseestudillo.kafka.producer.NewProducerTask;

public class ProdCon {

	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "new-kafka-topic";

		String brokersCSV = "localhost:9092";

		NewProducerTask producer = new NewProducerTask(brokersCSV, topic);
		NewConsumerTask consumer = new NewConsumerTask(brokersCSV, "groupId", topic);

		new Thread(producer).start();
		new Thread(consumer).start();

		Thread.sleep(15000);

		producer.stop();
		consumer.stop();

		System.exit(0);
	}
}
