package com.orkhangasanov.tutorial_2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkArchitectureDemo {
    public static void main(String[] args) {
        // Step 1: Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("SparkArchitectureDemo")
                .setMaster("local[*]"); // run locally using all cores
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Step 2: Create an RDD
        JavaRDD<String> lines = sc.parallelize(Arrays.asList(
                "Apache Spark is fast",
                "Apache Spark is powerful",
                "Big data with Spark"
        ), 3); // force 3 partitions

        // Step 3: Transformations
        // Narrow transformation: flatMap (no shuffle)
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // Narrow transformation: mapToPair (still local to partition)
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        // Wide transformation: reduceByKey (shuffle boundary!)
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

        // Step 4: Action
        wordCounts.collect().forEach(System.out::println);

        // Keep app alive a bit so you can check Spark UI
        try {
            System.out.println("Go to Spark UI at http://localhost:4040");
            Thread.sleep(200000); // 20 seconds pause
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.close();
    }
}
