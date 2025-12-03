package com.orkhangasanov.tutorial_6;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class DataFrameFromList {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DataFrameFromList")
                .master("local[*]")
                .getOrCreate();

        // Example data
        List<Person> people = Arrays.asList(
                new Person("Alice", 30),
                new Person("Bob", 25),
                new Person("Charlie", 35)
        );

        // Convert list to Dataset
        Dataset<Row> df = spark.createDataFrame(people, Person.class);

        df.show();
        df.printSchema();

        spark.stop();
    }

    public static class Person implements java.io.Serializable {
        private String name;
        private int age;

        public Person() {}
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
        public String getName() { return name; }
        public int getAge() { return age; }
    }
}

