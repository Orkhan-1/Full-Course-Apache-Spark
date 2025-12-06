package com.orkhangasanov.tutorial_7;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSQLBasics {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLBasics")
                .master("local[*]")
                .getOrCreate();

        // Read JSON data
        Dataset<Row> df = spark.read().option("multiline", "true").json("people.json");

        // Register DataFrame as temporary SQL view
        df.createOrReplaceTempView("people");

        // Run SQL query
        Dataset<Row> adults = spark.sql("SELECT name, age FROM people WHERE age >= 30 " +
                "                                                            ORDER BY age DESC");
        adults.show();

        spark.stop();
    }
}
