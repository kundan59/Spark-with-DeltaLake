package org.examples;

import io.delta.tables.DeltaTable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public final class Upsert {

    private final static String SPARK_APPLICATION_NAME = "update&delete";
    private final static String SPARK_APPLICATION_RUNNING_MODE = "local";
    private final static String FILE_PATH = "src/main/resources/original.csv";
    private final static String NEW_FILE_PATH = "src/main/resources/updates.csv";
    private final static String STORE_FILE_PATH = "sparkdata/upsertdata";

    public static void main(String[] args) {

        // Turn off spark's default logger
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder().appName(SPARK_APPLICATION_NAME)
                .master(SPARK_APPLICATION_RUNNING_MODE)
                .getOrCreate();

        //Reading csv file and creating salaryDataFrame table.
        Dataset<Row> salaryDataFrame = sparkSession.read().option("header", true).csv(FILE_PATH);

        // writing salaryDataFrame to STORE_FILE_PATH
        salaryDataFrame.write().option("header", true).format("delta").save(STORE_FILE_PATH);

        sparkSession.read().format("delta").load(STORE_FILE_PATH).show();

        // Delta Table
        DeltaTable deltaTable = DeltaTable.forPath(sparkSession, STORE_FILE_PATH);

        // Delete record of Lisa Langford
//        deltaTable.delete("EmployeeName = 'Langford, Lisa'");
//        deltaTable.toDF().show();

        //Updated job title of employ has salaryDataId 10
//        deltaTable.updateExpr(
//                "SalaryDataID = 10",
//                new HashMap<String, String>() {{
//                    put("JobTitle", "'Traffic Planning'");
//                }}
//        );
//        deltaTable.toDF().show();

        //Reading csv file and creating new salaryDataFrame table.
        Dataset<Row> newSalaryDataFrame = sparkSession.read().option("header", true).csv(NEW_FILE_PATH);

        DeltaTable.forPath(sparkSession, STORE_FILE_PATH)
                .as("salaryDeltaTable")
                .merge(
                        newSalaryDataFrame.as("newSalaryDataFrame"),
                        "salaryDeltaTable.SalaryDataID = newSalaryDataFrame.SalaryDataID")
                .whenMatched()
                .updateExpr(
                        new HashMap<String, String>() {{
                            put("Department" , "newSalaryDataFrame.Department");
                            put("JobTitle" , "newSalaryDataFrame.JobTitle");
                        }})
                .whenNotMatched()
                .insertExpr(
                        new HashMap<String, String>() {{
                            put("SalaryDataID", "newSalaryDataFrame.SalaryDataID");
                            put("CalendarYear", "newSalaryDataFrame.CalendarYear");
                            put("EmployeeName", "newSalaryDataFrame.EmployeeName");
                            put("Department", "newSalaryDataFrame.Department");
                            put("JobTitle", "newSalaryDataFrame.JobTitle");
                            put("AnnualRate", "newSalaryDataFrame.AnnualRate");
                            put("RegularRate", "newSalaryDataFrame.RegularRate");
                            put("OvertimeRate", "newSalaryDataFrame.OvertimeRate");
                            put("IncentiveAllowance", "newSalaryDataFrame.IncentiveAllowance");
                            put("Other", "newSalaryDataFrame.Other");
                            put("YearToDate", "newSalaryDataFrame.YearToDate");
                        }})
                .execute();
        deltaTable.toDF().show();

        //close Spark Session
        sparkSession.close();

    }
}
