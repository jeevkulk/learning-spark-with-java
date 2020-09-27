package jeevkulk.sparkcore.example;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DataAnalysisUsingRDD {

    Logger logger = LogManager.getLogger(DataAnalysisUsingRDD.class);

    public static void main(String[] args) {
        DataAnalysisUsingRDD dataAnalysisUsingRDD = new DataAnalysisUsingRDD();
        dataAnalysisUsingRDD.analyseData();
    }

    public void analyseData() {
        StopWatch sw = new StopWatch();
        sw.start();
        JavaSparkContext sc = getSparkContext();
        JavaRDD<String> allColumnsRdd = sc.textFile(getDataFile());

        sw.stop();
        logger.info("===========> Total time taken to load RDDs: " + sw.getTime(TimeUnit.SECONDS) + " seconds");
        filterFor(allColumnsRdd,"BurgerKing-Anchorage,AK");
        countByKey(allColumnsRdd);
        countByAfterGrouping(allColumnsRdd);
        regionWiseCount(allColumnsRdd);
    }

    private JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("App Name");
        return new JavaSparkContext(conf);
    }

    private String getDataFile() {
        return this.getClass().getClassLoader().getResource("burgerking.csv").getFile();
    }

    private void filterFor(JavaRDD<String> rdd, String filterCriteria) {
        StopWatch sw = new StopWatch();
        sw.start();
        rdd = rdd.filter(s -> s.contains(filterCriteria));
        sw.stop();
        logger.info("===========> Total count of lines filtered: " + rdd.count() + " in time " + sw.getTime(TimeUnit.MICROSECONDS) + " msecs");
    }

    private void countByAfterGrouping(JavaRDD<String> rdd) {
        StopWatch sw = new StopWatch();
        sw.start();

        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(data -> {
            String[] dataArr = data.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            return new Tuple2(dataArr[2], 1);
        });
        pairRDD = pairRDD.reduceByKey((data1, data2) -> data1 + data2);

        List<Tuple2<String, Integer>> list = pairRDD.collect();

        logger.info(list.size());
        list.forEach(tuple -> {
            logger.info(tuple._1 + " : " + tuple._2);
        });
        sw.stop();
        logger.info("===========> Time taken: " + sw.getTime(TimeUnit.MICROSECONDS) + " msecs");
    }

    private void countByKey(JavaRDD<String> rdd) {
        StopWatch sw = new StopWatch();
        sw.start();

        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(data -> {
            String[] dataArr = data.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            return new Tuple2(dataArr[2], 1);
        });
        Map<String, Long> map = pairRDD.countByKey();
        map.forEach((k, v) -> logger.info(k+" --- "+v));
        sw.stop();
        logger.info("===========> Time taken: " + sw.getTime(TimeUnit.MICROSECONDS) + " msecs");
    }

    private void regionWiseCount(JavaRDD<String> rdd) {
        StopWatch sw = new StopWatch();
        sw.start();
        JavaRDD<String> regionalCentresRDD = rdd.map(data -> data.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")[2]);
        JavaPairRDD<String, Integer> pairRDD = regionalCentresRDD.mapToPair(data -> new Tuple2(data, 1));
        JavaPairRDD<String, Integer> pairRDDCounts = pairRDD.reduceByKey((a, b) -> a + b);
        logger.info(pairRDDCounts.collect());
        sw.stop();
        logger.info("===========> Time taken: " + sw.getTime(TimeUnit.MICROSECONDS) + " msecs");
    }
}

