package com.orkhangasanov.tutorial_7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class SparkSQLJoins {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLJoins")
                .master("local[*]")
                .getOrCreate();

        // Sample People DataFrame
        List<Person> peopleList = Arrays.asList(
                new Person(1, "Alice", 30),
                new Person(2, "Bob", 25),
                new Person(3, "Charlie", 35)
        );
        Dataset<Row> peopleDF = spark.createDataFrame(peopleList, Person.class);
        peopleDF.createOrReplaceTempView("people");

        // Sample Departments DataFrame
        List<Department> deptList = Arrays.asList(
                new Department(1, "Engineering"),
                new Department(2, "HR")
        );
        Dataset<Row> deptDF = spark.createDataFrame(deptList, Department.class);
        deptDF.createOrReplaceTempView("departments");

        // SQL Join
        Dataset<Row> joined = spark.sql(
                "SELECT p.name, p.age, d.name as dept " +
                        "FROM people p " +
                        "LEFT JOIN departments d ON p.id = d.id"
        );
        joined.show();

        spark.stop();
    }

    // Serializable JavaBean for People
    public static class Person implements java.io.Serializable {
        private int id;
        private String name;
        private int age;

        public Person() {}
        public Person(int id, String name, int age) { this.id = id; this.name = name; this.age = age; }
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
    }

    // Serializable JavaBean for Departments
    public static class Department implements java.io.Serializable {
        private int id;
        private String name;

        public Department() {}
        public Department(int id, String name) { this.id = id; this.name = name; }
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }
}

/*

                                   Under the Hood

All these SQL queries are converted by the Catalyst Optimizer into optimized execution plans,
and Tungsten Engine executes them efficiently using parallelism and memory optimizations.
This is why Spark SQL can handle massive datasets faster than traditional SQL engines

                                        RECAP

-Spark SQL lets you query structured data using familiar SQL syntax.
-You can register DataFrames as temporary views or tables.
-Supports filtering, aggregation, ordering, and joining multiple tables.
-All queries are optimized and executed efficiently with Catalyst and Tungsten.


*/
