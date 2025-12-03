package com.orkhangasanov.tutorial_5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.parallelize(Arrays.asList(
                "Apache Spark is fast",
                "Spark is powerful",
                "Big data is fun"
        ));

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        // reduceByKey() - Combine values by key
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum);

        System.out.println("Word Counts: " + counts.collect());

        sc.stop();
    }
}
