package com.joseestudillo.kafka.old.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class KafkaBrokerManager {

	private static final Logger log = Logger.getLogger(KafkaBrokerManager.class);

	private List<KafkaServerStartable> brokers;

	public KafkaBrokerManager() {
		brokers = new ArrayList<KafkaServerStartable>();
	}

	public void addAndStart(Properties brokerConfig) {
		log.info(String.format("Config: %s", brokerConfig));
		KafkaConfig config = new KafkaConfig(brokerConfig);
		KafkaServerStartable newBroker = new KafkaServerStartable(config);
		log.info(String.format("Starting the broker with Id: %s", newBroker.serverConfig().brokerId()));
		newBroker.startup();
		this.brokers.add(newBroker);
		log.info(String.format("Broker with Id: %s started", newBroker.serverConfig().brokerId()));
	}

	public void addAndStartAll(List<Properties> brokerConfigs) {
		for (Properties brokerConfig : brokerConfigs) {
			this.addAndStart(brokerConfig);
		}
	}

	public void stopAll() {
		log.info("Sending the stop signal to all the brokers");
		for (KafkaServerStartable broker : this.brokers) {
			log.info(String.format("Stopping Broker id: %s", broker.serverConfig().brokerId()));
			broker.shutdown();
		}
		log.info("The stop sign has been sent to all the brokers");
	}

	public void awaitForTermination() {
		log.info("Awaiting for all the brokers to stop");
		for (KafkaServerStartable broker : this.brokers) {
			log.info(String.format("Waiting for the Broker id: %s to stop", broker.serverConfig().brokerId()));
			broker.awaitShutdown();
		}
		log.info("All brokers have been stopped");
	}

	public List<String> getBrokerHostList() {
		List<String> hosts = new ArrayList<>();
		for (KafkaServerStartable broker : this.brokers) {
			String hostname = broker.serverConfig().hostName();
			if (hostname == null) {
				hostname = "localhost";
			}
			hosts.add(String.format("%s:%s", hostname, broker.serverConfig().port()));
		}
		return hosts;
	}

	public String getBrokerHostCSV() {
		return String.join(",", getBrokerHostList());
	}

}