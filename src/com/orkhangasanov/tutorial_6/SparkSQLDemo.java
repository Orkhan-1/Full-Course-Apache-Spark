package com.orkhangasanov.tutorial_6;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSQLDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLDemo")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().option("multiline", "true").json("people.json");
        df.createOrReplaceTempView("people");

        Dataset<Row> result = spark.sql("SELECT name, age FROM people WHERE age > 23");
        result.show();

        spark.stop();
    }
}

