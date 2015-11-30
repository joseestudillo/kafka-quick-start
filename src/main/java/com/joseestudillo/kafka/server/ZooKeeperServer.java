package com.joseestudillo.kafka.server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
 * Runs a Zookeeper server programmatically.
 * 
 * @author Jose Estudillo
 *
 */
public class ZooKeeperServer {

	public static final Logger log = Logger.getLogger(ZooKeeperServer.class);

	QuorumPeerConfig quorumPeerConfiguration;
	ZooKeeperServerMain zooKeeperServer;

	public static void getClient() {

	}

	public ZooKeeperServer(Properties zkProperties) throws FileNotFoundException, IOException {
		quorumPeerConfiguration = new QuorumPeerConfig();
		try {
			quorumPeerConfiguration.parseProperties(zkProperties);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public void start() {
		Thread t = new Thread() {
			public void run() {
				zooKeeperServer = new ZooKeeperServerMain();
				try {
					log.info(String.format("Starting ZooKeeper. Thread Id: %s", this.getId()));
					final ServerConfig configuration = new ServerConfig();
					configuration.readFrom(quorumPeerConfiguration);
					zooKeeperServer.runFromConfig(configuration);
				} catch (IOException e) {
					log.error("ZooKeeper Failed", e);
				}
			}
		};
		t.start();
	}

	public void stop() {
		//this.zooKeeperServer. TODO there is no method to stop it
	}
}