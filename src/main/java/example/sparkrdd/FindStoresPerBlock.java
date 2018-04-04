package example.sparkrdd;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class FindStoresPerBlock {

    Logger logger = LogManager.getLogger(FindStoresPerBlock.class);

    public static void main(String[] args) {
        FindStoresPerBlock findStoresPerBlock = new FindStoresPerBlock();
        findStoresPerBlock.doService();
    }

    private void doService() {
        JavaSparkContext sc = getSparkContxt();
        JavaRDD<String> rdd = sc.textFile(getFile("burgerking.csv"));
        getBlockWiseStores(rdd);
        getBlockWiseCountOfStores(rdd);
    }

    private void getBlockWiseStores(JavaRDD<String> rdd) {
        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(data -> {
            String[] str = data.split(",");
            double latitude = Math.ceil(Double.parseDouble(str[0]));
            double longitude = Math.ceil(Double.parseDouble(str[1]));
            return new Tuple2<String, String>(String.valueOf(latitude) + ":" + String.valueOf(longitude), str[2]);
        });

        JavaPairRDD<String, String> reducedByNamePairRDD = pairRDD.reduceByKey((a, b) -> {
            return a + "," + b;
        });
        logger.info(reducedByNamePairRDD.collect());
    }

    private void getBlockWiseCountOfStores(JavaRDD<String> rdd) {
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(data -> {
            String[] str = data.split(",");
            double latitude = Math.ceil(Double.parseDouble(str[0]));
            double longitude = Math.ceil(Double.parseDouble(str[1]));
            return new Tuple2<String, Integer>(String.valueOf(latitude) + ":" + String.valueOf(longitude), 1);
        });

        JavaPairRDD<String, Integer> reducedByCountPairRDD = pairRDD.reduceByKey((a, b) -> {
            return a + b;
        });
        logger.info(reducedByCountPairRDD.collect());
    }

    private JavaSparkContext getSparkContxt() {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("FindStorePerBlock");
        return new JavaSparkContext(sparkConf);
    }

    private String getFile(String filename) {
        return this.getClass().getClassLoader().getResource(filename).getFile();
    }
}
