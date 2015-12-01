package com.joseestudillo.kafka.utils;

import java.io.IOException;
import java.util.Properties;

public class KafkaUtils {

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
}
