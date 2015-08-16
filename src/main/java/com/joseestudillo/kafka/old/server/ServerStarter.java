package com.joseestudillo.kafka.old.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.joseestudillo.kafka.utils.KafkaUtils;

public class ServerStarter {

	private static final Logger log = Logger.getLogger(ServerStarter.class);

	public static class BrokerConfigFactory {

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
				configs.add(generateProperties(String.valueOf(baseId + i), basePort + i));
			}
			return configs;
		}
	}

	KafkaBrokerManager brokerManager;
	int nBrokers;
	Properties zooKeeperDefaultConfig;
	Properties brokerDefaultConfig;

	public ServerStarter(int nBrokers) {
		this.nBrokers = nBrokers;
	}

	public ServerStarter(Properties zooKeeperDefaultConfig, Properties brokerDefaultConfig, int nBrokers) {
		this(nBrokers);
		this.zooKeeperDefaultConfig = zooKeeperDefaultConfig;
		this.brokerDefaultConfig = brokerDefaultConfig;
	}

	public String getBrokerCSV() {
		return this.brokerManager.getBrokerHostCSV();
	}

	public void start() throws IOException, InterruptedException {
		if (zooKeeperDefaultConfig == null) {
			zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/zookeeper.properties");
		}
		zooKeeperDefaultConfig.setProperty("host.name", "localhost");
		if (brokerDefaultConfig == null) {
			brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath("/config/server.properties");
		}
		log.info("Starting ZooKeeper...");
		ZooKeeperServer zookeeper = new ZooKeeperServer(zooKeeperDefaultConfig);
		zookeeper.start();

		log.info("Waiting for ZooKeeper to be ready...");
		Thread.sleep(1000); //Trick as the ZooKeeper implementation doesn't offer much
		log.info("ZooKeeper ready");

		log.info("Starting Kafka Brokers...");
		BrokerConfigFactory brokerConfigFactory = new BrokerConfigFactory(brokerDefaultConfig);
		List<Properties> brokerConfigs = brokerConfigFactory.generateBrokerConfigs(0, 9090, this.nBrokers);
		brokerManager = new KafkaBrokerManager();
		brokerManager.addAndStartAll(brokerConfigs);

		log.info(String.format("Brokers available: %s", brokerManager.getBrokerHostCSV()));
	}

	public void stop() {
		log.info("Attenting to stop the brokers...");
		if (brokerManager != null) {
			brokerManager.stopAll();
			brokerManager.awaitForTermination();
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		ServerStarter server = new ServerStarter(3);
		server.start();
		log.info("Waiting for a minute...");
		Thread.sleep(600000);
		server.stop();

	}
}
