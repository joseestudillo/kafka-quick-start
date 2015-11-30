package com.joseestudillo.kafka.old.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class OldHighLevelConsumerTask implements Runnable {

	private static final Logger log = Logger.getLogger(OldHighLevelConsumerTask.class);
	private static final AtomicInteger instanceSequencer = new AtomicInteger(0);

	private static class KafkaStreamConsumer implements Runnable {

		private static final Logger log = Logger.getLogger(KafkaStreamConsumer.class);

		KafkaStream<byte[], byte[]> stream;

		public KafkaStreamConsumer(KafkaStream<byte[], byte[]> stream) {
			this.stream = stream;
		}

		@Override
		public void run() {
			log.info(String.format("Kafka Stream Consumer. Thread Id: %s. Started.", Thread.currentThread().getId()));
			String msg;
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
			while (consumerIte.hasNext()) {
				msg = new String(consumerIte.next().message());
				log.info(String.format("Kafka Stream Consumer. Thread Id: %s. Message Consumed: %s", Thread.currentThread().getId(), msg));
			}
			log.info(String.format("Kafka Stream Consumer. Thread Id: %s. DONE.", Thread.currentThread().getId()));
		}

	}

	int instanceId;
	String topic;
	int threadsPerTopic;
	ConsumerConnector consumer;

	public OldHighLevelConsumerTask(String topic, String groupId, int threadsPerTopic, String zookeeper) {

		instanceId = instanceSequencer.getAndIncrement();
		this.topic = topic;
		this.threadsPerTopic = threadsPerTopic;

		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("client.id", String.valueOf(System.currentTimeMillis()));
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");

		this.consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	@Override
	public void run() {
		log.info(String.format("Consumer %s started. Thread Id: %s", this.instanceId, Thread.currentThread().getId()));
		try {
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
			topicMap.put(topic, this.threadsPerTopic);

			log.info(String.format("Topics Map: %s", topicMap));
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);
			List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);

			log.info(String.format("%s streams found for the topic %s", streamList.size(), this.topic));
			ExecutorService executor = Executors.newFixedThreadPool(this.threadsPerTopic);
			for (final KafkaStream<byte[], byte[]> stream : streamList) {
				executor.submit(new KafkaStreamConsumer(stream));
			}
		} catch (Exception e) {
			log.error(String.format("Consumer %s. Thread Id: %s. Stopped.", this.instanceId, Thread.currentThread().getId()), e);
		}
		log.info(String.format("Consumer %s. Thread Id: %s. Done.", this.instanceId, Thread.currentThread().getId()));
	}

	public void stop() {
		this.consumer.shutdown();
	}
}
