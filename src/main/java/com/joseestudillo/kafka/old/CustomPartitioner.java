package com.joseestudillo.kafka.old;

import org.apache.log4j.Logger;

import kafka.producer.Partitioner;

public class CustomPartitioner implements Partitioner {

	private static final Logger log = Logger.getLogger(CustomPartitioner.class);

	@Override
	public int partition(Object key, int nPartitions) {
		int result = key.hashCode() % nPartitions;
		log.info(String.format("partition(%s, %s) -> %s", key, nPartitions, result));
		return result;
	}
}