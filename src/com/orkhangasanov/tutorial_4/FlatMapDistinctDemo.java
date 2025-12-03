package com.orkhangasanov.tutorial_4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;

public class FlatMapDistinctDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("FlatMapDistinctDemo")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> sentences = sc.parallelize(Arrays.asList(
                "Apache Spark is fast",
                "Spark is powerful",
                "Big data is fun"
        ));

        JavaRDD<String> words = sentences.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaRDD<String> uniqueWords = words.distinct();

        System.out.println("Unique words: " + uniqueWords.collect());

        sc.stop();
    }
}

