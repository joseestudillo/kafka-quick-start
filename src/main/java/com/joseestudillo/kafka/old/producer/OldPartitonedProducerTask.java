package com.joseestudillo.kafka.old.producer;

import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Implementation of a producer with partitioning
 * 
 * @author Jose Estudillo
 *
 */

public class OldPartitonedProducerTask implements Runnable {

	private static Logger log = Logger.getLogger(OldPartitonedProducerTask.class);

	private static final Properties BASE_PROPS = new Properties();

	static {
		BASE_PROPS.put("serializer.class", "kafka.serializer.StringEncoder");
		BASE_PROPS.put("request.required.acks", "1");
	}

	private String topic;
	Producer<Integer, String> producer;
	private String partitionerClass;

	public OldPartitonedProducerTask(String brokerCSV, String topic, Class<? extends kafka.producer.Partitioner> partitionerClass) {
		this.topic = topic;
		this.partitionerClass = partitionerClass.getCanonicalName();
		this.initializeProducer(this.generateProperties(brokerCSV));
	}

	protected Properties generateProperties(String brokerCSV) {
		Properties properties = new Properties();
		properties.putAll(BASE_PROPS);
		properties.put("metadata.broker.list", brokerCSV);
		//This is the only difference with a regular producer in terms of configurations
		//TODO I'm having problems instantiating this class when the producers are launched
		properties.put("partitioner.class", this.partitionerClass);
		return properties;
	}

	protected void initializeProducer(Properties properties) {
		ProducerConfig config = new ProducerConfig(properties);
		producer = new Producer<Integer, String>(config);
	}

	@Override
	public void run() {
		log.info(String.format("Producer started. Thread Id: %s", Thread.currentThread().getId()));
		int sequence = 0;
		try {
			String msg;
			while (!Thread.interrupted()) {
				msg = String.format("Sequence message: %s", sequence++);
				//for this case we will use the sequence as a message key
				KeyedMessage<Integer, String> keyedMessage = new KeyedMessage<Integer, String>(topic, sequence, msg);
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
