package com.orkhangasanov.tutorial_9;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;

public class StructuredStreamingFileExample {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("StructuredStreamingFileExample")
                .master("local[*]")
                .getOrCreate();

        // Define schema explicitly
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
                .option("header", "true")
                .schema(schema)
                .csv("stream_data/");

        // Simple transformation: select columns
        Dataset<Row> transformed = df.select("product_name", "category");

        // Write output to console
        transformed.writeStream()
                .format("console")
                .outputMode("append")
                .start()
                .awaitTermination();
    }
}