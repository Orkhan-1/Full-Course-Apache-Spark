package com.orkhangasanov.tutorial_8;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CsvToParquet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CsvToParquet")
                .master("local[*]")
                .getOrCreate();

        // Read CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .csv("products.csv");

        // Write as Parquet
        df.write()
                .mode("overwrite")
                .parquet("products.parquet");

        System.out.println("Parquet file created successfully!");

        spark.stop();
    }
}

/**
 PARQUET FILE STRUCTURE & EXAMPLE EXPLANATION
 ============================================

 EXAMPLE TABULAR DATA:
 +------------+----------------+-------------+-------+
 | product_id | product_name   | category    | stock |
 +------------+----------------+-------------+-------+
 | 1          | Smartphone     | Electronics | 150   |
 | 2          | Laptop         | Electronics | 80    |
 | 3          | Desk Chair     | Furniture   | 200   |
 | 4          | Table          | Furniture   | 120   |
 | 5          | Headphones     | Electronics | 300   |
 | 6          | Coffee Maker   | Appliances  | 100   |
 | 7          | Blender        | Appliances  | 180   |
 | 8          | Notebook       | Stationery  | 500   |
 | 9          | Pen            | Stationery  | 1000  |
 | 10         | Monitor        | Electronics | 60    |
 +------------+----------------+-------------+-------+

 HOW IT'S STORED:
 ================

 1. ROW-BASED FORMAT (CSV):
 -----------------------
 1,Smartphone,Electronics,150
 2,Laptop,Electronics,80
 3,Desk Chair,Furniture,200
 4,Table,Furniture,120
 5,Headphones,Electronics,300
 6,Coffee Maker,Appliances,100
 7,Blender,Appliances,180
 8,Notebook,Stationery,500
 9,Pen,Stationery,1000
 10,Monitor,Electronics,60

 Pros: Simple, human-readable
 Cons: Inefficient for analytics, must read entire rows

 2. COLUMNAR FORMAT (PARQUET):
 --------------------------
 Column 1 (product_id):   [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
 Column 2 (product_name): ["Smartphone", "Laptop", "Desk Chair", "Table",
 "Headphones", "Coffee Maker", "Blender",
 "Notebook", "Pen", "Monitor"]
 Column 3 (category):     ["Electronics", "Electronics", "Furniture",
 "Furniture", "Electronics", "Appliances",
 "Appliances", "Stationery", "Stationery",
 "Electronics"]
 Column 4 (stock):        [150, 80, 200, 120, 300, 100, 180, 500, 1000, 60]

 Each column stored and compressed separately!

 PARQUET FILE STRUCTURE:
 ========================

 File: products.parquet (Actually a directory structure):
 --------------------------------------------------------
 products.parquet/
 ├── _SUCCESS                    # Success marker file
 ├── _common_metadata            # Common schema info
 ├── _metadata                   # Global metadata
 ├── part-00000-<hash>.snappy.parquet  # Data file 1
 ├── part-00001-<hash>.snappy.parquet  # Data file 2
 └── part-00002-<hash>.snappy.parquet  # Data file 3

 */
