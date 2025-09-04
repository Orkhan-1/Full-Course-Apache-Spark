package com.orkhangasanov;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public class WordCountApp {
    public static void main(String[] args) {
        // 1. Create Spark configuration object
        SparkConf conf = new SparkConf()
                .setAppName("WordCountApp")
                .setMaster("local[*]");

        // 2. Initialize Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. Read input text file and create an RDD (Resilient Distributed Dataset)
        JavaRDD<String> lines = sc.textFile("input.txt");

        long wordCount = lines
                .flatMap(line -> java.util.Arrays.asList(line.split(" ")).iterator())
                .count();

        System.out.println("Total words: " + wordCount);

        sc.close();
    }
}
