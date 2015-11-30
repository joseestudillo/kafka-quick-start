package com.joseestudillo.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import com.joseestudillo.kafka.old.partitioner.CustomPartitioner;

/**
 * Implementation of a producer with partitioning
 * 
 * @author Jose Estudillo
 *
 */

public class NewPartitionedProducerTask implements Runnable {

	private static Logger log = Logger.getLogger(NewPartitionedProducerTask.class);

	private static final Properties BASE_PROPS = new Properties();

	static {}

	private String topic;
	KafkaProducer<String, String> producer;
	private CustomPartitioner partitioner = null;

	public NewPartitionedProducerTask(String brokerCSV, String topic, CustomPartitioner partitioner) {
		this.topic = topic;
		this.partitioner = partitioner;
		this.initializeProducer(this.generateProperties(brokerCSV));
	}

	protected Properties generateProperties(String brokerCSV) {
		Properties properties = new Properties();
		properties.putAll(BASE_PROPS);
		properties.put("bootstrap.servers", brokerCSV);
		properties.put("key.serializer", StringSerializer.class.getCanonicalName());
		properties.put("value.serializer", StringSerializer.class.getCanonicalName());
		//This is the only difference with a regular producer in terms of configurations

		return properties;
	}

	protected void initializeProducer(Properties properties) {
		producer = new KafkaProducer<String, String>(properties);
	}

	@Override
	public void run() {
		log.info(String.format("Producer started. Thread Id: %s", Thread.currentThread().getId()));
		int sequence = 0;
		try {
			String msg;
			while (true) {
				msg = String.format("Sequence message: %s", sequence++);
				//for this case we will use the sequence as a message key
				ProducerRecord<String, String> keyedMessage;
				String key = String.valueOf(sequence);
				String value = msg;
				if (this.partitioner == null) {
					keyedMessage = new ProducerRecord<String, String>(topic, key, msg);
				} else {
					Integer partition = this.partitioner.partition(key);
					keyedMessage = new ProducerRecord<String, String>(topic, partition, key, value);
					log.info(String.format("Producer. Thread Id: %s. Sending:{topic: %s, partition: %s, key: %s, value: %s}", Thread.currentThread().getId(),
							topic, partition, key, value));
				}
				producer.send(keyedMessage);
				log.info(String.format("Producer. Thread Id: %s. Sent: %s", Thread.currentThread().getId(), msg));
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			log.error("Producer Stopped: ", e);
		}
	}

	public void stop() {
		producer.close();
	}

}
