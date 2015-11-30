package com.joseestudillo.kafka.old.producer;

import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class OldProducerTask implements Runnable {

	private static Logger log = Logger.getLogger(OldProducerTask.class);

	private static final Properties BASE_PROPS = new Properties();

	static {
		BASE_PROPS.put("serializer.class", "kafka.serializer.StringEncoder");
		BASE_PROPS.put("request.required.acks", "1");
	}

	private String topic;
	Producer<String, String> producer;

	public OldProducerTask(String topic) {
		this.topic = topic;
	}

	public OldProducerTask(String brokerCSV, String topic) {
		this(brokerCSV, topic, false);
	}

	public OldProducerTask(String brokerCSV, String topic, boolean autocreateTopic) {
		this(topic);
		this.initializeProducer(generateProperties(brokerCSV, autocreateTopic));
	}

	protected Properties generateProperties(String brokerCSV, boolean autocreateTopic) {
		Properties properties = new Properties();
		properties.putAll(BASE_PROPS);
		properties.put("metadata.broker.list", brokerCSV);
		if (autocreateTopic) {
			properties.put("auto.create.topics.enable", "true");
		}
		return properties;
	}

	protected void initializeProducer(Properties properties) {
		ProducerConfig config = new ProducerConfig(properties);
		producer = new Producer<String, String>(config);
	}

	@Override
	public void run() {
		log.info(String.format("Producer started. Thread Id: %s", Thread.currentThread().getId()));
		int sequence = 0;
		try {
			String msg;
			while (!Thread.interrupted()) {
				msg = String.format("Sequence message: %s %s", sequence++, System.currentTimeMillis());
				KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topic, msg);
				producer.send(keyedMessage);
				log.info(String.format("Producer. Thread Id: %s. Sent: %s", Thread.currentThread().getId(), msg));
				Thread.sleep(500);
			}
		} catch (Exception e) {
			log.error("Producer Stopped: ", e);
		}
	}

	public void stop() {
		producer.close();
	}

}
