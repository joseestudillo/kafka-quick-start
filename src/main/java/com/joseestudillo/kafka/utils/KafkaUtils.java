package com.joseestudillo.kafka.utils;

import java.io.IOException;
import java.util.Properties;

public class KafkaUtils {
	public static Properties loadPropertyFileFromClassPath(String path) throws IOException {
		Properties properties = new Properties();
		properties.load(KafkaUtils.class.getResourceAsStream(path));
		return properties;
	}
}
