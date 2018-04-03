package example.sparkrdd;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Demonstrates ways of creating RDDs
 */
public class CreatingRDD {

    Logger logger = LogManager.getLogger(CreatingRDD.class);

    public static void main(String[] args) {
        CreatingRDD creatingRDD = new CreatingRDD();
        creatingRDD.createRDDUsingParallelize();
        creatingRDD.createRDDUsingExternalSource();
        creatingRDD.createRDDUsingOtherRDD();
    }

    /**
     * This way of creating RDD is just for testing purpose, should not be used in live code.
     *
     * Notes:
     * 1. Comparator implementation with Serialization
     * 2. top method on RDD
     * 3. first method on RDD does not remove the element from RDD
     */
    private void createRDDUsingParallelize() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MyApp");
        JavaSparkContext context = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("English", "Hindi", "Marathi", "Maths", "Science", "Geography", "History");
        JavaRDD<String> javaRDD = context.parallelize(list);
        List<String> top1 = javaRDD.top(3, (Comparator<String> & Serializable)Comparator.reverseOrder());
        top1.forEach(logger::info);

        String first = javaRDD.first();
        logger.info("First element= "+first);

        List<String> top2 = javaRDD.top(3, (Comparator<String> & Serializable)Comparator.reverseOrder());
        top2.forEach(logger::info);
    }

    /**
     * Refer code DataAnalysisUsingRDDOperations
     */
    private void createRDDUsingExternalSource() {
    }

    /**
     * Refer code DataAnalysisUsingRDDOperations
     */
    private void createRDDUsingOtherRDD() {
    }
}
