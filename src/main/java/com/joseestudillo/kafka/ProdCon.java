package com.joseestudillo.kafka;

import java.io.IOException;

import com.joseestudillo.kafka.consumer.NewConsumerFactory;
import com.joseestudillo.kafka.consumer.NewConsumerTask;
import com.joseestudillo.kafka.producer.NewProducerFactory;
import com.joseestudillo.kafka.producer.NewProducerTask;

public class ProdCon {

	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "new-kafka-topic";

		String brokersCSV = "localhost:9092";

		NewProducerTask producer = new NewProducerTask(NewProducerFactory.newInstance(brokersCSV), topic);
		NewConsumerTask consumer = new NewConsumerTask(NewConsumerFactory.newInstance(brokersCSV, "groupId", topic));

		new Thread(producer).start();
		new Thread(consumer).start();

		Thread.sleep(20000);

		producer.stop();
		consumer.stop();

		System.exit(0);
	}
}
