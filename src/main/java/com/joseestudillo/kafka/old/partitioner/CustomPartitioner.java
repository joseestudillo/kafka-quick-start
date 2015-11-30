package com.joseestudillo.kafka.old.partitioner;

import org.apache.log4j.Logger;

import kafka.producer.Partitioner;

public class CustomPartitioner implements Partitioner {

	private static final Logger log = Logger.getLogger(CustomPartitioner.class);

	int nPartitions;

	public CustomPartitioner() {

	}

	public CustomPartitioner(int nPartitions) {
		this.nPartitions = nPartitions;
	}

	public CustomPartitioner(Integer nPartitions) {

	}

	@Override
	public int partition(Object key, int nPartitions) {
		int result = key.hashCode() % nPartitions;
		log.info(String.format("partition(%s, %s) -> %s", key, nPartitions, result));
		return result;
	}

	//method added to cover the case of the new implementation where the producer must define the partition in advance
	public Integer partition(Object key) {
		int result = key.hashCode() % this.nPartitions;
		log.info(String.format("partition(%s, %s) -> %s", key, this.nPartitions, result));
		return result;
	}
}
