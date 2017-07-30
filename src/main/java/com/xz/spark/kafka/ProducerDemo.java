package com.xz.spark.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerDemo {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zk.connect", "study1:2181,study2:2181,study3:2181");
		props.put("metadata.broker.list", "study1:9092,study2:9092,study3:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 0; i <= 100; i++)
			producer.send(new KeyedMessage<String, String>("study", "test " + i));
	}
}