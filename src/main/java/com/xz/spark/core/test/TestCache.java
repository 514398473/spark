package com.xz.spark.core.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 测试持久化
 * 
 * @author Administrator
 *
 */
public class TestCache {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("cache").setMaster("local[1]");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> persist = javaSparkContext.textFile("LICENSE.txt").cache();
		// JavaRDD<String> persist =
		// javaSparkContext.textFile("LICENSE.txt").persist(StorageLevel.MEMORY_ONLY());

		long begin = System.currentTimeMillis();
		long count = persist.count();
		long end = System.currentTimeMillis();
		System.out.println(end - begin + ":" + count);

		begin = System.currentTimeMillis();
		count = persist.count();
		end = System.currentTimeMillis();
		System.out.println(end - begin + ":" + count);

		javaSparkContext.close();
	}

}
