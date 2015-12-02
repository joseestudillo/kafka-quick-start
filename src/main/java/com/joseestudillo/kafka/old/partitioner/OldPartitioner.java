package com.joseestudillo.kafka.old.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Simple implementation of a partitioner for Kafka's old implementation.
 * 
 * @author Jose Estudillo
 *
 */
public class OldPartitioner implements Partitioner {

	public OldPartitioner(VerifiableProperties props) {}

	public int partition(Object key, int nPartitions) {
		return key.hashCode() % nPartitions;
	}

}