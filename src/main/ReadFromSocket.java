package main;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class ReadFromSocket {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("Word Count");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		
//		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
//		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
//
//		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
//		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKeyAndWindow((a, b)-> a + b, Durations.seconds(30));
//
//		JavaDStream<Long> countDStream = wordCounts.count();
//		JavaPairDStream<String, Integer> sortedCounts = wordCounts.transformToPair(x -> x.sortByKey(true));
//
//		countDStream.print();
//		sortedCounts.print();

		JavaDStream<String> lines = jsc.textFileStream("/home/trannguyenhan/Desktop/desktop");
		lines.print();
		
		jsc.start();             
		jsc.awaitTermination();
	}
}
