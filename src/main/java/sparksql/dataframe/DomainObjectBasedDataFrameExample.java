package sparksql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import java.io.Serializable;

public class DomainObjectBasedDataFrameExample {

    public static void main(String[] args) {
        DomainObjectBasedDataFrameExample example = new DomainObjectBasedDataFrameExample();
        example.createAndShowDataFrame();
    }

    private void createAndShowDataFrame() {
        SparkSession sparkSession = getSparkSession();
        JavaRDD<String> stringRDD = sparkSession.sparkContext().textFile(getDataFile(), 1).toJavaRDD();
        JavaRDD<Franchise> franchiseRDD = stringRDD.map(data -> {
            String[] dataArr = data.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            return new Franchise(Double.parseDouble(dataArr[0]), Double.parseDouble(dataArr[1]), dataArr[2], dataArr[3]);
        });
        Dataset<Row> bkDataFrame = sparkSession.createDataFrame(franchiseRDD, Franchise.class);
        try {
            bkDataFrame.createTempView("burger_king1");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        Dataset<Row> results = sparkSession.sql("select * from burger_king1");
        results.show();
    }

    private SparkSession getSparkSession() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("DataFrame Example")
                .config(new SparkConf().setMaster("local").setAppName("DataFrame Example"))
                .getOrCreate();
        return sparkSession;
    }

    private String getDataFile() {
        return this.getClass().getClassLoader().getResource("burgerking.csv").getFile();
    }

    public static class Franchise implements Serializable {
        private double latitude;
        private double longitude;
        private String name;
        private String address;

        public Franchise() {
        }

        public Franchise(double latitude, double longitude, String name, String address) {
            this.latitude = latitude;
            this.longitude = longitude;
            this.name = name;
            this.address = address;
        }

        public double getLatitude() {
            return latitude;
        }

        public void setLatitude(double latitude) {
            this.latitude = latitude;
        }

        public double getLongitude() {
            return longitude;
        }

        public void setLongitude(double longitude) {
            this.longitude = longitude;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }
}

