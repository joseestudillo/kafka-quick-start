package com.joseestudillo.kafka.old;

import java.io.IOException;

import com.joseestudillo.kafka.old.consumer.OldHighLevelConsumerTask;
import com.joseestudillo.kafka.old.producer.OldProducerTask;

/**
 * Launch 1 kafka producer and 1 kafka consumer. This class expect a running zookeeper server and at least one broker.
 * 
 * @author Jose Estudillo
 *
 */
public class OldProdCon {
	public static void main(String[] args) throws InterruptedException, IOException {
		String zookeeper = args.length > 0 ? args[0] : "localhost:2181";
		String brokersCSV = args.length > 1 ? args[1] : "localhost:9092";
		String topic = args.length > 2 ? args[2] : "kafka-topic";
		String groupId = args.length > 3 ? args[3] : "groupId";

		OldProducerTask producer = new OldProducerTask(brokersCSV, topic, true);
		OldHighLevelConsumerTask consumer = new OldHighLevelConsumerTask(topic, groupId, 1, zookeeper);

		new Thread(producer).start();
		Thread.sleep(1000);
		new Thread(consumer).start();

		Thread.sleep(10000);

		producer.stop();
		consumer.stop();

		System.out.println("DONE");
		System.exit(0);
	}
}
