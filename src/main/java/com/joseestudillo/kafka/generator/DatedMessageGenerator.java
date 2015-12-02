package com.joseestudillo.kafka.generator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class DatedMessageGenerator implements MessageGenerator {

	private static final String MSG_FORMAT = "%s, %s";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private AtomicLong sequence = new AtomicLong(0);

	@Override
	public String getNext() {
		return String.format(MSG_FORMAT, sdf.format(new Date()), sequence.incrementAndGet());
	}

}
