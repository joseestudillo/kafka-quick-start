package com.joseestudillo.kafka;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import com.joseestudillo.kafka.consumer.NewConsumerFactory;
import com.joseestudillo.kafka.consumer.NewConsumerTask;
import com.joseestudillo.kafka.partitioner.NewPartitioner;
import com.joseestudillo.kafka.producer.NewProducerFactory;
import com.joseestudillo.kafka.producer.NewProducerTask;

/**
 * Example of how to use a partitioner with a partitioned topic.
 * 
 * Notice that the partitioned topic must be created in advance and the brokers are harcoded.
 * 
 * @author jo186021
 *
 */
public class ProdConPartitionedTopic {

	public static void main(String[] args) throws InterruptedException, IOException {
		String topic = "partitioned-topic";
		String groupId = "groupId";
		String brokersCSV = "localhost:9092,localhost:9093,localhost:9094";

		int nProducers = 1;
		int nConsumers = 3;

		List<KafkaProducer<String, String>> producers = NewProducerFactory.newInstances(brokersCSV, nProducers, NewPartitioner.class);
		List<KafkaConsumer<String, String>> consumers = NewConsumerFactory.newInstances(brokersCSV, groupId, topic, nConsumers, false);
		ExecutorService executor = Executors.newFixedThreadPool(nConsumers + nProducers);

		for (KafkaProducer<String, String> producer : producers) {
			executor.submit(new NewProducerTask(producer, topic));
		}
		for (KafkaConsumer<String, String> consumer : consumers) {
			executor.submit(new NewConsumerTask(consumer));
		}

		Thread.sleep(20000);

		executor.shutdown();

		System.exit(0);
	}
}
