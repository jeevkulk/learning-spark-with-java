package example;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.concurrent.TimeUnit;

public class DataAnalysis {

    Logger logger = LogManager.getLogger(DataAnalysis.class);

    private final String filepath = "E:\\technology_workspace\\data\\";

    public static void main(String[] args) {
        DataAnalysis dataAnalysis = new DataAnalysis();
        dataAnalysis.analyseData();
    }

    public void analyseData() {
        StopWatch sw = new StopWatch();
        sw.start();
        JavaSparkContext sc = getSparkContext();
        JavaRDD<String> rdd = sc.textFile(filepath + "papers.lst");
        rdd = rdd.repartition(4);
        sw.stop();
        logger.info("Total time taken to load RDDs: " + sw.getTime(TimeUnit.SECONDS) + " seconds");
        filterFor(rdd,"1996");
    }

    private JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("App Name");
        return new JavaSparkContext(conf);
    }

    private void filterFor(JavaRDD<String> rdd, String years) {
        StopWatch sw = new StopWatch();
        sw.start();
        rdd = rdd.filter(s -> s.contains(years));
        sw.stop();
        logger.info("Total count of lines filtered: " + rdd.count() + " in time " + sw.getTime(TimeUnit.MILLISECONDS) + " msecs");
    }

    /*private void yearWiseCount(JavaRDD<String> rdd) {
        StopWatch sw = new StopWatch();
        sw.start();
        rdd = rdd ;
        sw.stop();
        logger.info("Total count of lines filtered: " + rdd.count() + " in time " + sw.getTime(TimeUnit.NANOSECONDS) + " nano-seconds");
    }*/
}
