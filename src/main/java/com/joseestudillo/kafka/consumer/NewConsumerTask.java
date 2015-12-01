package com.joseestudillo.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

public class NewConsumerTask implements Runnable {

	private static final Logger log = Logger.getLogger(NewConsumerTask.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);
	private KafkaConsumer<String, String> consumer;
	private String topic;

	public NewConsumerTask(String brokerCSV, String groupId, String topic) {
		this.topic = topic;
		this.initializeProducer(generateProperties(brokerCSV, groupId));
	}

	protected Properties generateProperties(String brokerCSV, String groupId) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", brokerCSV);
		properties.put("group.id", groupId);
		//properties.put("enable.auto.commit", "false");
		//properties.put("auto.commit.interval.ms", "1000");
		//properties.put("session.timeout.ms", "30000");
		//the deserializers are mandatory in the new implementation
		properties.put("key.deserializer", StringDeserializer.class.getCanonicalName());
		properties.put("value.deserializer", StringDeserializer.class.getCanonicalName());

		return properties;
	}

	protected void initializeProducer(Properties properties) {
		this.consumer = new KafkaConsumer<String, String>(properties);
	}

	public void run() {
		try {
			log.info(String.format("Subscribig Consumer Task Id: %s to the topic: %s", Thread.currentThread().getId(), this.topic));
			consumer.subscribe(Arrays.asList(this.topic));
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(10000);
				for (ConsumerRecord<String, String> record : records) {
					log.info(String.format("Consumer Thread Id: %s. Message Consumed. offset = %d, key = %s, value = %s",
							Thread.currentThread().getId(),
							record.offset(),
							record.key(),
							record.value()));
				}
			}
		} catch (WakeupException e) {
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			log.info(String.format("Closing Consumer Task Id: %s", Thread.currentThread().getId()));
			consumer.close();
		}
	}

	public void shutdown() {
		log.info(String.format("Shutting down consumer Task Id: %s", Thread.currentThread().getId()));
		closed.set(true);
		consumer.wakeup();
	}

	public void stop() {
		this.shutdown();
	}
}
