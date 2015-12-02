package com.joseestudillo.kafka.consumer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

public class NewConsumerTask implements Runnable {

	private static final Logger log = Logger.getLogger(NewConsumerTask.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);

	private KafkaConsumer<String, String> consumer;

	public NewConsumerTask(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}

	public void run() {
		try {
			log.info(String.format("Consumer Task Id: %s with topics: %s", Thread.currentThread().getId(), consumer.subscription()));
			for (String subscribedTopic : consumer.subscription()) {
				log.info(String.format("Consumer Task Id: %s. Subscribed to the Topic: %s. Topic Info: %s",
						Thread.currentThread().getId(),
						subscribedTopic,
						consumer.listTopics().get(subscribedTopic)));
			}
			while (!closed.get()) {
				log.info(String.format("Getting Consumer Records. Task Id: %s for the topics: %s",
						Thread.currentThread().getId(),
						this.consumer.subscription()));
				//TODO how to make this timeout?
				ConsumerRecords<String, String> records = consumer.poll(5000);
				for (ConsumerRecord<String, String> record : records) {
					log.info(String.format("Consumer Thread Id: %s. Message Consumed. offset = %d, partition = %d, key = %s, value = %s",
							Thread.currentThread().getId(),
							record.offset(),
							record.partition(),
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
