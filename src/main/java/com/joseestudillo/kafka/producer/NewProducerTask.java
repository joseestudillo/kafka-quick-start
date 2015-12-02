package com.joseestudillo.kafka.producer;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import kafka.producer.Partitioner;

public class NewProducerTask implements Runnable {

	private static Logger log = Logger.getLogger(NewProducerTask.class);

	private KafkaProducer<String, String> producer;

	private String topic;

	public NewProducerTask(KafkaProducer<String, String> producer, String topic) {
		this.producer = producer;
		this.topic = topic;
	}

	public NewProducerTask(KafkaProducer<String, String> producer, String topic, Partitioner partitioner) {
		this.producer = producer;
		this.topic = topic;
	}

	@Override
	public void run() {
		long id = Thread.currentThread().getId();
		log.info(String.format("Producer started. Thread Id: %s", id));
		int sequence = 0;
		try {
			String msg;
			while (!Thread.interrupted()) {
				msg = String.format("Sequence message: %s", sequence++);
				ProducerRecord<String, String> keyedMessage = new ProducerRecord<String, String>(topic, msg);
				//new ProducerRecord<String, String>(topic, key, msg); //to set a key
				//new ProducerRecord<String, String>(topic, partition, key, value); // to allow manual partitioning.
				log.info(String.format("Producer. Thread Id: %s. Sending...", id));
				Future<RecordMetadata> future = producer.send(keyedMessage);
				log.info(String.format("Producer. Thread Id: %s. Waiting for metadata...", id));
				RecordMetadata metadata = future.get();
				log.info(String.format("Producer. Thread Id: %s. Metadata received: %s", id, metadata));
				log.info(String.format("Producer. Thread Id: %s. Sent: %s", id, msg));
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			log.error(String.format("Producer Stopped: Thread Id: %s", id), e);
			producer.close();
		}
	}

	public void stop() {
		producer.close();
	}

}
