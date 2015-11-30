package com.joseestudillo.kafka.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Generates Kafka brokers configs to allow to launch several brokers.
 * 
 * @author Jose Estudillo
 *
 */
public class BrokerConfigFactory {

	private static final String KAFKA_BROKER_LOG_DIR_PTTRN = "/tmp/kafka/logs/%s";

	private Properties commonProperties;

	public BrokerConfigFactory(Properties commonProperties) {
		this.commonProperties = commonProperties;
	}

	public BrokerConfigFactory(String zookeeperHost, int zookeeperPort, Properties commonProperties) {
		this(commonProperties);
		commonProperties.put("zookeeper.connect", String.format("%s:%s", zookeeperHost, zookeeperPort));
	}

	public BrokerConfigFactory(String zookeeperHost, int zookeeperPort) {
		this(zookeeperHost, zookeeperPort, new Properties());
	}

	private Properties generateProperties(String id, int port) {
		Properties properties = new Properties();
		properties.putAll(this.commonProperties);
		properties.put("broker.id", id);
		properties.put("port", String.valueOf(port));
		properties.put("log.dir", String.format(KAFKA_BROKER_LOG_DIR_PTTRN, id));
		properties.put("log.dirs", String.format("/tmp/kafka-logs/%s", id));

		return properties;
	}

	public List<Properties> generateBrokerConfigs(int baseId, int basePort, int nBrokers) {
		List<Properties> configs = new ArrayList<Properties>();
		for (int i = 0; i < nBrokers; i++) {
			configs.add(this.generateProperties(String.valueOf(baseId + i), basePort + i));
		}
		return configs;
	}
}