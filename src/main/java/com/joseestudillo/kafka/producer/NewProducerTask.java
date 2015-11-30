package com.joseestudillo.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

public class NewProducerTask implements Runnable {

	private static Logger log = Logger.getLogger(NewProducerTask.class);

	private String topic;
	KafkaProducer<String, String> producer;

	public NewProducerTask(String brokerCSV, String topic) {
		this.topic = topic;
		this.initializeProducer(generateProperties(brokerCSV));
	}

	protected Properties generateProperties(String brokerCSV) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", brokerCSV);
		//the serializers are mandatory in the new implementation
		properties.put("key.serializer", StringSerializer.class.getCanonicalName());
		properties.put("value.serializer", StringSerializer.class.getCanonicalName());

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
			while (!Thread.interrupted()) {
				msg = String.format("Sequence message: %s", sequence++);
				ProducerRecord<String, String> keyedMessage = new ProducerRecord<String, String>(topic, msg);
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
