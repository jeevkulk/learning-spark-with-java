package sparkrdd.transformation;

import domain.Course;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum WideTransformations {

    INSTANCE;

    Logger logger = LogManager.getLogger(WideTransformations.class);

    private JavaSparkContext context;

    private JavaSparkContext getJavaSparkContext() {
        if (context == null) {
            synchronized (this) {
                if (context == null) {
                    SparkConf conf = new SparkConf().setAppName("wideTransformations").setMaster("local");
                    context = new JavaSparkContext(conf);
                }
            }
        }
        return context;
    }

    /**
     * Intersection of two RDDs
     * @param list1
     * @param list2
     * @return
     */
    public JavaRDD<Course> getIntersectionRDD(List<Course> list1, List<Course> list2) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> rdd1 = context.parallelize(list1);
        JavaRDD<Course> rdd2 = context.parallelize(list2);
        return rdd1.intersection(rdd2);
    }
}
