package com.joseestudillo.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.joseestudillo.kafka.partitioner.NewPartitioner;

public class NewProducerFactory {

	public static List<KafkaProducer<String, String>> newInstances(String brokerCSV, int nProducers) {
		List<KafkaProducer<String, String>> producers = new ArrayList<>();
		for (int i = 0; i < nProducers; i++) {
			producers.add(newInstance(brokerCSV));
		}
		return producers;
	}

	public static List<KafkaProducer<String, String>> newInstances(String brokerCSV, int nProducers, Class<NewPartitioner> partitionerClass) {
		List<KafkaProducer<String, String>> producers = new ArrayList<>();
		for (int i = 0; i < nProducers; i++) {
			producers.add(newInstance(brokerCSV, partitionerClass));
		}
		return producers;
	}

	public static KafkaProducer<String, String> newInstance(String brokerCSV) {
		return newInstance(brokerCSV, null);
	}

	public static KafkaProducer<String, String> newInstance(String brokerCSV, Class<?> partitionerClass) {
		Properties properties = generateProperties(brokerCSV, partitionerClass);
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	private static Properties generateProperties(String brokerCSV, Class<?> partitionerClass) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", brokerCSV);
		//the serializers are mandatory in the new implementation
		properties.put("key.serializer", StringSerializer.class.getCanonicalName());
		properties.put("value.serializer", StringSerializer.class.getCanonicalName());
		//timeouts
		properties.put("timeout.ms", "15000");
		properties.put("metadata.fetch.timeout.ms", "15000");
		if (partitionerClass != null) {
			properties.put("partitioner.class", partitionerClass.getCanonicalName());
		}

		return properties;
	}

}
