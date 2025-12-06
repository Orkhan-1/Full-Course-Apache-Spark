package com.orkhangasanov.tutorial_7;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSQLAggregations {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLAggregations")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().option("multiline", "true").json("people.json");
        df.createOrReplaceTempView("people");

        // Count how many people per age
        Dataset<Row> ageCounts = spark.sql("SELECT age, COUNT(*) as count " +
                "FROM people GROUP BY age ORDER BY age");
        ageCounts.show();

        spark.stop();
    }
}
