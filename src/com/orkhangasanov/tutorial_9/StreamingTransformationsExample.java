package com.orkhangasanov.tutorial_9;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class StreamingTransformationsExample {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("StreamingTransformationsExample")
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
                .schema(schema)
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("stream_data/");

        // Filter rows where age > 25
        Dataset<Row> adults = df.filter(col("stock").gt(100));

        // Group by category
        Dataset<Row> agg = adults.groupBy("category").count();

        agg.writeStream()
                .format("console")
                .outputMode("complete") // full aggregated results
                .start()
                .awaitTermination();
    }
}
