package main.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class FraudDetection {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("Fraud Detection")
                .master("local[*]")
                .getOrCreate();

        // Define the schema of the transactions JSON
        StructType schema = new StructType()
                .add("transaction_id", "string")
                .add("user_id", "string")
                .add("amount", "double")
                .add("timestamp", "string");

        // Read transactions from Kafka
        Dataset<Row> transactions = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "transactions")
                .load()
                .selectExpr("CAST(value AS STRING) as transaction")
                .select(functions.from_json(functions.col("transaction"), schema).as("data"))
                .select("data.*");

        // Parse the timestamp column to TimestampType
        transactions = transactions.withColumn("timestamp", functions.col("timestamp").cast("timestamp"));

        // Apply a 1-minute window and aggregate the amounts per user
        Dataset<Row> windowedTransactions = transactions
                .groupBy(
                        functions.window(transactions.col("timestamp"), "1 minute"),
                        transactions.col("user_id")
                )
                .agg(functions.sum("amount").as("total_amount"));

        // Filter windows where the sum of amounts exceeds $1,000
        Dataset<Row> fraudNotifications = windowedTransactions.filter("total_amount > 2000");

        // Write potential fraud notifications to Kafka
        StreamingQuery query = fraudNotifications
                .selectExpr("CAST(window.start AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "potential_fraud_notifications")
                .option("checkpointLocation", "/path/to/checkpoint/dir")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        query.awaitTermination();
    }
}
