package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class BasicDataFrameOperations {

    public static void main(String[] args) {
        BasicDataFrameOperations example = new BasicDataFrameOperations();
        example.createAndShowDataFrameUsingRow();
    }

    private void createAndShowDataFrameUsingRow() {
        SparkSession sparkSession = getSparkSession();
        Dataset<Row> dataset = sparkSession.read().json(getDataFile());
        dataset.show();

        calculateYearlyPopulationGroupedBySex(dataset);
    }

    private String getDataFile() {
        return this.getClass().getClassLoader().getResource("population.json").getFile();
    }

    private SparkSession getSparkSession() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("DataFrame Example")
                .config(new SparkConf().setMaster("local").setAppName("DataFrame Example"))
                .getOrCreate();
        return sparkSession;
    }

    private void calculateYearlyPopulationGroupedBySex(Dataset<Row> dataset) {
        dataset.groupBy("year", "sex").sum("people").orderBy("year", "sex").show();
    }
}

