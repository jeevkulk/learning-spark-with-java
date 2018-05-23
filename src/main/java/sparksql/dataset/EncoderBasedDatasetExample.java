package sparksql.dataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import java.io.Serializable;

public class EncoderBasedDatasetExample {

    public static void main(String[] args) {
        EncoderBasedDatasetExample example = new EncoderBasedDatasetExample();
        example.createAndShowDataset();
    }

    private void createAndShowDataset() {
        SparkSession sparkSession = getSparkSession();
        JavaRDD<String> stringRDD = sparkSession.sparkContext().textFile(getDataFile(), 1).toJavaRDD();
        JavaRDD<Franchise> franchiseRDD = stringRDD.map(data -> {
            String[] dataArr = data.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            return new Franchise(Double.parseDouble(dataArr[0]), Double.parseDouble(dataArr[1]), dataArr[2], dataArr[3]);
        });
        Encoder<Franchise> franchiseEncoder = Encoders.bean(Franchise.class);
        Dataset<Franchise> bkDataset = sparkSession.createDataset(franchiseRDD.rdd(), franchiseEncoder);
        bkDataset.show();

        //Can be converted to DataFrames and vice versa
        try {
            bkDataset.createTempView("burger_king1");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        Dataset<Row> franchiseDF = sparkSession.sql("select * from burger_king1");
        franchiseDF.show();
        Dataset<Franchise> franchiseDS = franchiseDF.as(franchiseEncoder);
        franchiseDS.show();
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

