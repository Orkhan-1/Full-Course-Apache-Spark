package com.orkhangasanov.tutorial_9;

// File: StreamingWriteToDiskExample.java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class StreamingWriteToDiskExample {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("StreamingWriteToDiskExample")
                .master("local[*]")
                .getOrCreate();

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("product_id",
                DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("product_name",
                DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("category",
                DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("stock",
                DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = spark.readStream()
                .schema(schema)
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("stream_data/");

        df.writeStream()
                .format("parquet") // save as Parquet files
                .option("path", "output/stream_parquet")
                .option("checkpointLocation", "output/checkpoint") // required for exactly-once
                .start()
                .awaitTermination();
    }
}



/*

-Structured Streaming lets us treat live data as tables.

-We can read CSV, JSON, or Kafka streams.

-Apply transformations just like batch DataFrames.

-Output to console, files, or databases with fault tolerance.

-Structured Streaming makes Spark powerful for real-time analytics

*/
