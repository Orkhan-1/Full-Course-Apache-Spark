package com.orkhangasanov.tutorial_5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

public class GroupSortDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("GroupSortDemo")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> salesData = Arrays.asList(
                new Tuple2<>("apple", 50),
                new Tuple2<>("banana", 30),
                new Tuple2<>("apple", 70),
                new Tuple2<>("orange", 60),
                new Tuple2<>("banana", 40)
        );

        JavaPairRDD<String, Integer> sales = sc.parallelizePairs(salesData);

        // groupByKey() - group all values for the same key
        JavaPairRDD<String, Iterable<Integer>> grouped = sales.groupByKey();

        // reduceByKey() - sum all values for each key
        JavaPairRDD<String, Integer> totals = sales.reduceByKey(Integer::sum);

        // sortByKey() - alphabetical
        JavaPairRDD<String, Integer> sorted = totals.sortByKey();

        System.out.println("Grouped: " + grouped.collect());
        System.out.println("Totals: " + totals.collect());
        System.out.println("Sorted: " + sorted.collect());

        sc.stop();
    }
}

