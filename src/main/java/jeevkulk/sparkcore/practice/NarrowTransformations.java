package jeevkulk.sparkcore.practice;

import jeevkulk.sparkcore.practice.domain.Course;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
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
     * Filters out elective courses
     * @param courses
     * @return
     */
    public JavaRDD<Course> filterOutElectiveCourses(List<Course> courses) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> rdd = context.parallelize(courses);
        return rdd.filter(course -> !course.isElective());
    }
    /**
     * Demonstrates how to map JavaRDD<Course> to JavaRDD<String>
     * @param courses
     * @return
     */
    public JavaRDD<String> getAllCourseNamesUsingMap(List<Course> courses) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> rdd = context.parallelize(courses);
        return rdd.map(course -> course.getName());
    }

    /**
     * Uses flatMap to return all strings in all course objects
     * @param courses
     * @return
     */
    public JavaRDD<String> getAllCourseCodesUsingFlatMap(List<Course> courses) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> rdd = context.parallelize(courses);
        return rdd.flatMap(course -> {
            return Arrays.asList(course.getCode(), course.getCode(), course.getCode()).iterator();
        });
    }

    /**
     * Gets all course names using mapPartitions - mapPartition runs only once per partition
     * @param courses
     * @return
     */
    public JavaRDD<String> getAllCourseNamesUsingMapPartition(List<Course> courses) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> rdd = context.parallelize(courses);
        /*FlatMapFunction<Iterator<Course>, String> iteratorFlatMapFunction = new FlatMapFunction<Iterator<Course>, String>() {
            @Override
            public Iterator<String> call(Iterator<Course> courseIterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (courseIterator.hasNext()) {
                    list.add(courseIterator.next().getName());
                }
                return list.iterator();
            }
        };
        JavaRDD<String> rddStr = rdd.mapPartitions(iteratorFlatMapFunction);*/
        JavaRDD<String> rddStr = rdd.mapPartitions(courseIterator -> {
            List<String> list = new ArrayList<>();
            while (courseIterator.hasNext()) {
                list.add(courseIterator.next().getName());
            }
            return list.iterator();
        });
        return rddStr;
    }

    public JavaRDD<String> getAllCourseNamesUsingMapPartitionsWithIndex(List<Course> courses) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> rdd = context.parallelize(courses);

        /*Function2<Integer, Iterator<Course>, Iterator<String>> function2 = new Function2<Integer, Iterator<Course>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<Course> courseIterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (courseIterator.hasNext()) {
                    list.add(courseIterator.next().getName());
                }
                return list.iterator();
            }
        };
        JavaRDD<String> rddStr = rdd.mapPartitionsWithIndex(function2, true);*/

        JavaRDD<String> rddStr = rdd.mapPartitionsWithIndex((integer, courseIterator) -> {
            List<String> list = new ArrayList<>();
            while (courseIterator.hasNext()) {
                list.add(courseIterator.next().getName());
            }
            return list.iterator();
        }, true);
        return rddStr;
    }

    /**
     * Returns sample RDD based parameters passed to sample method
     * @param list
     * @param fraction
     * @return
     */
    public JavaRDD<Course> getSampleRDD(List<Course> list, boolean withReplacement, double fraction) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> rdd = context.parallelize(list);
        return rdd.sample(withReplacement, fraction, 1000L);
    }

    /**
     * Union of two RDDs
     * @param list1
     * @param list2
     * @return
     */
    public JavaRDD<Course> getUnionRDD(List<Course> list1, List<Course> list2) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> rdd1 = context.parallelize(list1);
        JavaRDD<Course> rdd2 = context.parallelize(list2);
        return rdd1.union(rdd2);
    }
}
