package com.xz.spark.streaming.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaDirectWordCount {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[1]");
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		Map<String, String> params = new HashMap<String, String>();
		params.put("metadata.broker.list", "node1:9092,node2:9092,node3:9092");

		Set<String> topics = new HashSet<String>();
		topics.add("wordcount");

		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(javaStreamingContext, String.class,
				String.class, StringDecoder.class, StringDecoder.class, params, topics);

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 6294528055679389900L;

			@Override
			public Iterable<String> call(Tuple2<String, String> line) throws Exception {
				return Arrays.asList(line._2.split(" "));
			}

		});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = -7187959654817167415L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		JavaPairDStream<String, Integer> wcs = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 965047811119387793L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		wcs.print();

		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
		javaStreamingContext.stop();
		javaStreamingContext.close();
	}
}
