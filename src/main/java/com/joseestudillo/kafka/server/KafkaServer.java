package com.joseestudillo.kafka.server;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.joseestudillo.kafka.utils.KafkaUtils;

/**
 * This class runs a Zookeeper server and a number of brokers programmatically.
 * 
 * The number of brokers launched is based on the given configuration.
 * 
 * @author Jose Estudilo
 *
 */
public class KafkaServer {

	private static final Logger log = Logger.getLogger(KafkaServer.class);

	private static final String DEFAULT_ZOOKEEPER_CFG = "/config/zookeeper.properties";
	private static final String DEFAULT_BROKER_CFG = "/config/server.properties";

	KafkaBrokerManager brokerManager;
	int nBrokers;
	Properties zooKeeperDefaultConfig;
	Properties brokerDefaultConfig;

	public KafkaServer(int nBrokers) {
		this.nBrokers = nBrokers;
	}

	public KafkaServer(Properties zooKeeperDefaultConfig, Properties brokerDefaultConfig, int nBrokers) {
		this(nBrokers);
		this.zooKeeperDefaultConfig = zooKeeperDefaultConfig;
		this.brokerDefaultConfig = brokerDefaultConfig;
	}

	public String getBrokerCSV() {
		return this.brokerManager.getBrokerHostCSV();
	}

	private void loadDefaulZookeeperConfig() throws IOException {
		this.zooKeeperDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath(DEFAULT_ZOOKEEPER_CFG);
	}

	private void loadDefaultBrokersConfig() throws IOException {
		this.brokerDefaultConfig = KafkaUtils.loadPropertyFileFromClassPath(DEFAULT_BROKER_CFG);
	}

	public void start() throws IOException, InterruptedException {
		if (this.zooKeeperDefaultConfig == null) {
			this.loadDefaulZookeeperConfig();
		}
		this.zooKeeperDefaultConfig.setProperty("host.name", "localhost");
		if (this.brokerDefaultConfig == null) {
			this.loadDefaultBrokersConfig();
		}
		log.info("Starting ZooKeeper...");
		ZooKeeperServer zookeeper = new ZooKeeperServer(this.zooKeeperDefaultConfig);
		zookeeper.start();

		log.info("Waiting for ZooKeeper to be ready...");
		Thread.sleep(1000); // Trick as the ZooKeeper implementation doesn't offer a way to wait for it to be ready
		log.info("ZooKeeper ready");

		log.info("Starting Kafka Brokers...");
		BrokerConfigFactory brokerConfigFactory = new BrokerConfigFactory(this.brokerDefaultConfig);
		List<Properties> brokerConfigs = brokerConfigFactory.generateBrokerConfigs(0, 9090, this.nBrokers);
		this.brokerManager = new KafkaBrokerManager();
		this.brokerManager.addAndStartAll(brokerConfigs);

		log.info(String.format("Brokers available: %s", this.brokerManager.getBrokerHostCSV()));
	}

	public void stop() {
		log.info("Attenting to stop the brokers...");
		if (this.brokerManager != null) {
			this.brokerManager.stopAll();
			this.brokerManager.awaitForTermination();
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		KafkaServer server = new KafkaServer(3);
		server.start();
		log.info("Waiting for a minute...");
		Thread.sleep(600000);
		log.info("Stopping Kafka");
		server.stop();

	}
}
