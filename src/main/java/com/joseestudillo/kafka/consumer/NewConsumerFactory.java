package com.joseestudillo.kafka.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class NewConsumerFactory {

	public static List<KafkaConsumer<String, String>> newInstances(String brokerCSV, String groupId, String topic, int nConsumers, boolean sameGroup) {
		List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
		String cosumerGroupId;
		for (int i = 0; i < nConsumers; i++) {
			cosumerGroupId = (sameGroup) ? groupId : groupId + i;
			consumers.add(newInstance(brokerCSV, cosumerGroupId, topic));
		}
		return consumers;
	}

	public static KafkaConsumer<String, String> newInstance(String brokerCSV, String groupId, String topic) {
		Properties properties = generateProperties(brokerCSV, groupId);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	private static Properties generateProperties(String brokerCSV, String groupId) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", brokerCSV);
		properties.put("group.id", groupId);
		//properties.put("enable.auto.commit", "false");
		//properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		//the deserializers are mandatory in the new implementation
		properties.put("key.deserializer", StringDeserializer.class.getCanonicalName());
		properties.put("value.deserializer", StringDeserializer.class.getCanonicalName());

		return properties;
	}

}
