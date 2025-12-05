package com.orkhangasanov.tutorial_4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;

public class ActionsDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ActionsDemo")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(10, 20, 30, 40, 50));

        // count()
        long count = nums.count();
        System.out.println("Count: " + count);

        // reduce()
        int sum = nums.reduce(Integer::sum);
        System.out.println("Sum: " + sum);

        // first()
        System.out.println("First element: " + nums.first());

        // take()
        System.out.println("Take 3 elements: " + nums.take(3));

        sc.stop();
    }
}

/*

A transformation creates a new RDD from an existing one
An action actually triggers computation and returns a result
*/

