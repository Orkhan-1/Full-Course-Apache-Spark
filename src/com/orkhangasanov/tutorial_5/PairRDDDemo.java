package com.orkhangasanov.tutorial_5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

public class PairRDDDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("PairRDDDemo")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> fruits = Arrays.asList("apple", "banana", "apple", "orange", "banana", "apple");

        JavaRDD<String> rdd = sc.parallelize(fruits);

        // mapToPair() - Create key-value pairs
        JavaPairRDD<String, Integer> pairs = rdd.mapToPair(fruit -> new Tuple2<>(fruit, 1));

        System.out.println("Pairs: " + pairs.collect());

        sc.stop();
    }
}
