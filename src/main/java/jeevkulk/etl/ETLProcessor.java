package jeevkulk.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class ETLProcessor {

    public static void main(String[] args) {
        //System.setProperty("hadoop.home.dir", "E:\\installations\\apache_hadoop\\hadoop-3.1.0\\");
        ETLProcessor processor = new ETLProcessor();
        processor.process();
    }

    public void process() {
        SparkSession sparkSession = getSparkSession();
        createInputParquetFile(sparkSession);
    }

    private void createInputParquetFile(SparkSession sparkSession) {
        JavaRDD<String> stringRDD = sparkSession.sparkContext().textFile(getDataInputFile(), 1).toJavaRDD();
        JavaRDD<Row> rowRDD = stringRDD.map(data -> {
            String[] dataArr = data.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            return RowFactory.create(Long.parseLong(dataArr[0]), dataArr[1], dataArr[2]);
        });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("EmpId", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("EmpName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("EmpLocation", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> empDataFrame = sparkSession.createDataFrame(rowRDD, schema);
        try {
            empDataFrame.printSchema();
            empDataFrame.show();
            empDataFrame.write()
                    .format("parquet")
                    .save("E:\\technology_workspace\\data\\out\\emp");
            //empDataFrame.coalesce(1).write().csv("E:\\technology_workspace\\data\\out\\emp_data.csv");
            //empDataFrame.write().parquet("E:\\technology_workspace\\data\\out\\emp_data.parquet");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private SparkSession getSparkSession() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("ETL POC")
                .config(new SparkConf().setMaster("local").setAppName("ETL POC"))
                .getOrCreate();
        return sparkSession;
    }

    private String getDataInputFile() {
        return this.getClass().getClassLoader().getResource("mulund_bus_group.csv").getFile();
    }
}
