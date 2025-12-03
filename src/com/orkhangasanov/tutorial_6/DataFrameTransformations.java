package com.orkhangasanov.tutorial_6;

// File: DataFrameTransformations.java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class DataFrameTransformations {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DataFrameTransformations")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().option("multiline", "true").json("people.json");

        df.show();
        df.select("name", "age").show();
        df.filter(col("age").gt(25)).show();
        df.groupBy("age").count().show();

        spark.stop();
    }
}
