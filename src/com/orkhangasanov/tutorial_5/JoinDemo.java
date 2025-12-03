package com.orkhangasanov.tutorial_5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

public class JoinDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JoinDemo")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> students = Arrays.asList(
                new Tuple2<>(1, "Alice"),
                new Tuple2<>(2, "Bob"),
                new Tuple2<>(3, "Charlie")
        );

        List<Tuple2<Integer, String>> courses = Arrays.asList(
                new Tuple2<>(1, "Math"),
                new Tuple2<>(2, "Science"),
                new Tuple2<>(4, "History")
        );

        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(students);
        JavaPairRDD<Integer, String> courseRDD = sc.parallelizePairs(courses);

        // Inner join
        JavaPairRDD<Integer, Tuple2<String, String>> joined = studentRDD.join(courseRDD);

        System.out.println("Joined: " + joined.collect());

        sc.stop();
    }
}

