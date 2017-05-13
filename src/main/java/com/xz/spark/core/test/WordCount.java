package com.xz.spark.core.test;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * java版的spark wordcount
 * @author Administrator
 *
 */
public class WordCount {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[1]");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> lines = javaSparkContext.textFile("file:///C:\\Users\\Administrator\\Desktop\\LICENSE.txt");
		
		/**
		 * 用空格切分数据
		 */
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 767105211104069269L;

			public Iterable<String> call(String line) throws Exception {
				String[] words = line.split(" ");
				return Arrays.asList(words);
			}
			
		});
		
		/**
		 * 把word 转换计数
		 */
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 3251181445943607544L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}

		});
		
		/**
		 * 计算每个word的count
		 */
		JavaPairRDD<String, Integer> wcs = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer count1, Integer count2) throws Exception {
				return count1 + count2;
			}
		});
		
		/**
		 * 反转key value
		 */
		JavaPairRDD<Integer, String> tempwcs = wcs.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, String> call(Tuple2<String, Integer> wc) throws Exception {
				
				return new Tuple2<Integer, String>(wc._2, wc._1);
			}
		});
		
		/**
		 * 倒序排列
		 */
		JavaPairRDD<Integer, String> sortedwcs = tempwcs.sortByKey(false);
		
		/**
		 * 反转key value
		 */
		JavaPairRDD<String, Integer> resultwcs = sortedwcs.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

			private static final long serialVersionUID = -2938500833946264148L;

			public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
				
				return new Tuple2<String, Integer>(t._2, t._1);
			}
		});
		
		/**
		 * 只取长度等于4的
		 */
		JavaPairRDD<String, Integer> filterwcs = resultwcs.filter(new Function<Tuple2<String,Integer>, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				return  v1._1 .length() == 4;
			}
		});
		
		/**
		 * 遍历结果
		 */
		filterwcs.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = -4239651059193261144L;

			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t.toString());
			}
			
		});
		
		javaSparkContext.close();
	}
}
