package com.joseestudillo.kafka.old.producer;

import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class OldProducer implements Runnable {

	private static Logger log = Logger.getLogger(OldProducer.class);

	private static final Properties BASE_PROPS = new Properties();

	static {
		BASE_PROPS.put("serializer.class", "kafka.serializer.StringEncoder");
		BASE_PROPS.put("request.required.acks", "1");
	}

	private String topic;
	Producer<String, String> producer;

	public OldProducer(String topic) {
		this.topic = topic;
	}

	public OldProducer(String brokerCSV, String topic) {
		this(topic);
		this.initializeProducer(generateProperties(brokerCSV));
	}

	protected Properties generateProperties(String brokerCSV) {
		Properties properties = new Properties();
		properties.putAll(BASE_PROPS);
		properties.put("metadata.broker.list", brokerCSV);
		return properties;
	}

	protected void initializeProducer(Properties properties) {
		ProducerConfig config = new ProducerConfig(properties);
		producer = new Producer<String, String>(config);
	}

	@Override
	public void run() {
		log.info(String.format("Producer started. Thread Id: %s", Thread.currentThread().getId()));
		try {
			String msg;
			while (!Thread.interrupted()) {
				msg = String.format("Timestamp message: %s", new Date().toString());
				KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topic, msg);
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
