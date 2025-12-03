package com.orkhangasanov.tutorial_4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TransformationsDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("TransformationsDemo")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(numbers);

        // Transformation 1: map()
        JavaRDD<Integer> squares = rdd.map(x -> x * x);

        // Transformation 2: filter()
        JavaRDD<Integer> filtered = squares.filter(x -> x > 10);

        // Action: collect()
        System.out.println("Numbers > 10: " + filtered.collect());

        sc.stop();
    }
}

