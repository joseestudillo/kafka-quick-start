package com.joseestudillo.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import kafka.producer.KeyedMessage;

public class JavaProducer {

	//in the case of asynchronous producers the
	// kafka.producer.Producer class handles the buffering of the producer’s data before the data is serialized and dispatched to the appropriate Kafka broker partition. 
	//Internally, the kafka.producer.async.ProducerSendThread instance dequeues the batch of messages
	//and kafka.producer.EventHandler serializes and dispatches the data. 
	//The Kafka producer configuration event.handler also provides the ability to define custom event handlers.

	public static void main(String[] args) {
		String topic = "java-producer-topic";

		ProducerConfig cfg = null;
		Map<String, Object> configs = new HashMap<String, Object>();
		Producer<String, String> producer = new KafkaProducer<String, String>(configs);

		KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topic, "keyed message without a key");
		keyedMessage = new KeyedMessage<String, String>(topic, "keyed message with a key");
	}
}
