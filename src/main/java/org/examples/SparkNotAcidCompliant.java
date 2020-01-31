package org.examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * SparkNotAcidCompliant is a class to illustrate spark is not ACID compliant.
 */
 public final class SparkNotAcidCompliant {

    private static final Logger LOGGER = Logger.getLogger(SparkNotAcidCompliant.class);
    private final static String SPARK_APPLICATION_NAME = "SparkAcidCompliantOrNot";
    private final static String SPARK_APPLICATION_RUNNING_MODE = "local";
    private final static String FILE_PATH = "sparkdata/deltalakedata";

    public static void main(String[] args) {
        // Turn off spark's default logger
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder().appName(SPARK_APPLICATION_NAME)
                .master(SPARK_APPLICATION_RUNNING_MODE)
                .getOrCreate();

        Dataset<Long> data = sparkSession.range(100, 200);

        //Job-1
        data.write().mode("overwrite").csv(FILE_PATH);
        LOGGER.info("records created by job-1: " + sparkSession.read().csv(FILE_PATH).count());

        //-Job-2
        try {
            sparkSession.range(100).map((MapFunction<Long, Integer>)
                    SparkNotAcidCompliant::getInteger, Encoders.INT())
                    .write().mode("overwrite").option("overwriteSchema", "true").csv(FILE_PATH);
        } catch (Exception e) {
            if (e.getCause() instanceof SparkException) {
                LOGGER.warn("failed while OverWriteData into data source", e.getCause());
            }
            throw new RuntimeException("Runtime exception!");
        }

        //close Spark Session
        sparkSession.close();
    }

    /**
     * Failed job in the middle.
     *
     * @param num number from the record.
     * @return Integer to be write in data Lake.
     */
    private static Integer getInteger(Long num) {
        if (num > 50) {
            throw new RuntimeException("Oops! Atomicity failed");
        }
        return Math.toIntExact(num);
    }
}
