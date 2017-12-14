package com.SparkPackage;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class SparkWordCount {

	public static void main(String args[]) throws IOException {
		SparkConf conf = new SparkConf().
				             setMaster("local[12]").
				             setAppName("WordCount").
				             set("spark.local.dir", "C:\\Work\\Spark\\Test\\temp").
				             set("spark.hadoop.validateOutputSpecs", "false");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("file:///C:/Work/Spark/test/*.txt");
		// input.foreach(e -> System.out.println(e));
		System.out.println("Line 19");

		JavaPairRDD<Integer, String> counts = input.
											  flatMap(s -> Arrays.asList(s.split(" ")).iterator()).
											  mapToPair(word -> new Tuple2<>(word.toLowerCase(), 1)).
											  reduceByKey((a, b) -> a + b).
											  mapToPair(item -> item.swap()).
											  sortByKey(false);

		/*
		 * errors.foreach(e -> { System.out.println(e); //fw.write(e); } );
		 */

		System.out.println("Line 26");
		counts.coalesce(1).saveAsTextFile("file:///C:/Work/Spark/Test/output/worCount");

		// fw.write(String.valueOf(errors.count()));

		Scanner scanner = new Scanner(System.in);

		scanner.nextLine();
		System.out.println("Line 37");
		sc.stop();
	}

}
