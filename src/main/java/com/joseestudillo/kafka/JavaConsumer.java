package com.joseestudillo.kafka;

import kafka.javaapi.consumer.SimpleConsumer;

public class JavaConsumer {

	public static void main(String[] args) {
		String host = "localhost";
		int port = 9090;
		int soTimeout = 1000;
		int bufferSize = 1000;
		String clientId = "";
		SimpleConsumer sc = new SimpleConsumer(host, port, soTimeout, bufferSize, clientId);
	}

}
