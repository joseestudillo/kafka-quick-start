package com.joseestudillo.kafka.partitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.log4j.Logger;

/**
 * Partitioner implementation example. This class decides to which partition a message goes to depending on the message's Key.
 * 
 * @author Jose Estudillo
 *
 */

// Example of how this would work manually from a producer
//
//  if (this.partitioner == null) { 
//    keyedMessage = new ProducerRecord<String, String>(topic, key, msg); 
//  } else { 
//    Integer partition =
//    this.partitioner.partition(key); keyedMessage = new ProducerRecord<String, String>(topic, partition, key, value); 
//  } 

public class NewPartitioner implements Partitioner {

	private static final Logger log = Logger.getLogger(NewPartitioner.class);

	@Override
	public void configure(Map<String, ?> configs) {
		log.info(String.format("Setting Partitioner config: %s", configs));
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		int nPartitions = cluster.partitionCountForTopic(topic);
		int partition = Math.abs(value.hashCode() % nPartitions);
		log.info(String.format("Partitioner (%s, %s) -> %s", value, nPartitions, partition));
		return partition;
	}

	@Override
	public void close() {
		log.info("Closing Partitioner");
	}
}
