package com.joseestudillo.kafka.old.producer;

import java.util.Properties;

public class OldPartitonedProducer extends OldProducer {

	private String partitionerClass;

	public OldPartitonedProducer(String brokerCSV, String topic, Class<? extends kafka.producer.Partitioner> partitionerClass) {
		super(topic);
		this.partitionerClass = partitionerClass.getCanonicalName();
		this.initializeProducer(this.generateProperties(brokerCSV));
	}

	protected Properties generateProperties(String brokerCSV) {
		Properties properties = super.generateProperties(brokerCSV);
		properties.put("metadata.broker.list", brokerCSV);
		properties.put("partitioner.class", this.partitionerClass);

		return properties;
	}

}
