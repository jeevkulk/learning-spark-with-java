package jeevkulk.sparkcore.practice;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Demonstrates ways of creating RDDs
 */
public class CreatingRDD {

    private Logger logger = LogManager.getLogger(CreatingRDD.class);

    private JavaSparkContext getJavaSparkContext() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MyApp");
        return new JavaSparkContext(conf);
    }

    /**
     * This way of creating RDD is just for testing purpose, should not be used in live code.
     */
    public JavaRDD<String> createRDDUsingParallelize(List<String> list) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<String> javaRDD = context.parallelize(list);
        return javaRDD;
    }

    /**
     * Refer code DataAnalysisUsingRDD
     */
    public void createRDDUsingExternalSource() {
    }

    /**
     * Refer code DataAnalysisUsingRDD
     */
    public void createRDDUsingOtherRDD() {
    }
}
