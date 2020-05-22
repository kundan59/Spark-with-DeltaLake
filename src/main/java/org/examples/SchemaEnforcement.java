package org.examples;

import io.delta.tables.DeltaTable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class SchemaEnforcement {
    private static final Logger LOGGER = Logger.getLogger(SparkNotAcidCompliant.class);
    private final static String SPARK_APPLICATION_NAME = "SparkSchemaEnforcement";
    private final static String SPARK_APPLICATION_RUNNING_MODE = "local";
    private final static String FILE_PATH = "src/main/resources/original.csv";
    private final static String STORE_FILE_PATH = "sparkdata/schemadata";

    public static void main(String[] args) {
        // Turn off spark's default logger
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder().appName(SPARK_APPLICATION_NAME)
                .master(SPARK_APPLICATION_RUNNING_MODE)
                .getOrCreate();

        //Reading csv file and creating salaryDataFrame table.
        Dataset<Row> salaryDataFrame = sparkSession.read().option("header", true).csv(FILE_PATH);

        //Creating Temporary view.
        salaryDataFrame.createOrReplaceTempView("salarytable");

        // writing salaryDataFrame to STORE_FILE_PATH
        salaryDataFrame.write().option("header", true).format("delta").save(STORE_FILE_PATH);
        salaryDataFrame.printSchema();

        // Query to select all columns with new column TotalSalary.
        Dataset<Row> totalSalaryDataFrame = sparkSession.sql("select SalaryDataID, EmployeeName, Department, JobTitle, " +
              "AnnualRate, RegularRate, OvertimeRate, IncentiveAllowance, Other, YearToDate," +
              " COALESCE(AnnualRate,0) + COALESCE(YearToDate,0)  AS TotalSalary from salarytable");
        totalSalaryDataFrame.printSchema();

        // Appending  totalSalaryDataFrame to  STORE_FILE_PATH
       totalSalaryDataFrame.write().mode("append").format("delta").save(STORE_FILE_PATH);

        //close Spark Session
        sparkSession.close();
    }
}
