package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class BasicDataFrameOperations {

    public static void main(String[] args) {
        BasicDataFrameOperations example = new BasicDataFrameOperations();
        example.createAndShowDatasetUsingRow();
    }

    private void createAndShowDatasetUsingRow() {
        SparkSession sparkSession = getSparkSession();
        Dataset<Row> dataset = sparkSession.read().json(getDataFile());
        dataset.show();
    }

    private SparkSession getSparkSession() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("DataFrame Example")
                .config(new SparkConf().setMaster("local").setAppName("DataFrame Example"))
                .getOrCreate();
        return sparkSession;
    }

    private String getDataFile() {
        return this.getClass().getClassLoader().getResource("population.json").getFile();
    }
}

