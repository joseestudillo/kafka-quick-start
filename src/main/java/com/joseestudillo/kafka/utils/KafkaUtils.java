package com.joseestudillo.kafka.utils;

import java.io.IOException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

public class KafkaUtils {

	private static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 5000;
	private static final int DEFAULT_ZK_CONN_TIMEOUT_MS = 5000;

	/**
	 * Load a property file from the classpath. The file path path must include / at the beginning.
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static Properties loadPropertyFileFromClassPath(String path) throws IOException {
		Properties properties = new Properties();
		properties.load(KafkaUtils.class.getResourceAsStream(path));
		return properties;
	}

	/**
	 * Creates a topic programatically. Deletes the topic if exists.
	 * 
	 * @param zkServers
	 * @param topic
	 */
	public static void createTopic(String zkServers, String topic) {
		createTopic(zkServers, topic, 1, 1, new Properties());
	}

	public static void createTopic(String zkServers, String topic, int nPartitions, int replicationFactor) {
		createTopic(zkServers, topic, nPartitions, replicationFactor, new Properties());
	}

	//TODO Implement a proper solution for topic creation
	/**
	 * Creates a topic programatically. Deletes the topic if exists.
	 * 
	 * @param zkServers
	 * @param topic
	 * @param nPartitions
	 * @param replicationFactor
	 * @param topicConfig
	 */
	public static void createTopic(String zkServers, String topic, int nPartitions, int replicationFactor, Properties topicConfig) {
		ZkConnection zkConnection = new ZkConnection(zkServers, DEFAULT_ZK_SESSION_TIMEOUT_MS);
		ZkClient zkClient = new ZkClient(zkConnection, DEFAULT_ZK_CONN_TIMEOUT_MS);
		ZkUtils zkUtil = new ZkUtils(zkClient, zkConnection, false);
		try {
			AdminUtils.deleteTopic(zkUtil, topic);
		} catch (Exception e) {
			e.printStackTrace();
		}
		AdminUtils.createTopic(zkUtil, topic, nPartitions, replicationFactor, topicConfig);
	}
}
