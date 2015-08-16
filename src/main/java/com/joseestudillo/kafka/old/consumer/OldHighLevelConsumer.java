package com.joseestudillo.kafka.old.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class OldHighLevelConsumer implements Runnable {

	private static final Logger log = Logger.getLogger(OldHighLevelConsumer.class);

	String topic;
	int threadsPerTopic;
	ConsumerConnector consumer;

	public OldHighLevelConsumer(String topic, String groupId, int threadsPerTopic, String zookeeper) {

		this.topic = topic;
		this.threadsPerTopic = threadsPerTopic;

		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");

		this.consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	@Override
	public void run() {
		log.info(String.format("Consumer started. Thread Id: %s", Thread.currentThread().getId()));
		try {
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
			topicMap.put(topic, this.threadsPerTopic);

			log.info(String.format("Topics Map: %s", topicMap));
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);
			List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);

			//TODO this structure doesn't make any sense, review!!!
			for (final KafkaStream<byte[], byte[]> stream : streamList) {
				String msg;
				ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
				while (consumerIte.hasNext()) {
					msg = new String(consumerIte.next().message());
					log.info(String.format("Consumer. Thread Id: %s. Message Consumed: %s", Thread.currentThread().getId(), msg));
				}
			}
		} catch (Exception e) {
			log.error(String.format("Consumer. Thread Id: %s. Stopped.", Thread.currentThread().getId()), e);
		}
		log.info(String.format("Consumer. Thread Id: %s. Done.", Thread.currentThread().getId()));
	}

	public void stop() {
		this.consumer.shutdown();
	}
}
