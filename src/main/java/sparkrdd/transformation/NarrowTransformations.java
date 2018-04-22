package sparkrdd.transformation;

import domain.Course;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public enum NarrowTransformations {

    INSTANCE;

    Logger logger = LogManager.getLogger(NarrowTransformations.class);

    private JavaSparkContext context;

    private JavaSparkContext getJavaSparkContext() {
        if (context == null) {
            synchronized (this) {
                if (context == null) {
                    SparkConf conf = new SparkConf().setAppName("narrowTransformations").setMaster("local");
                    context = new JavaSparkContext(conf);
                }
            }
        }
        return context;
    }

    /**
     * Demonstrates how to map JavaRDD<Course> to JavaRDD<String>
     * @param courses
     * @return
     */
    public JavaRDD<String> mapToCourseName(List<Course> courses) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> rdd = context.parallelize(courses);
        return rdd.map(course -> course.getName());
    }
}
