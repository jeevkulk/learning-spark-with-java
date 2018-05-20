package sparkcore.action;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public enum RDDFirstTopDemo {

    INSTANCE;

    private Logger logger = LogManager.getLogger(RDDFirstTopDemo.class);

    private JavaSparkContext context = null;

    private JavaSparkContext getJavaSparkContext() {
        if (context == null) {
            synchronized (this) {
                if (context == null) {
                    SparkConf conf = new SparkConf().setMaster("local").setAppName("RDDFirstTopDemo");
                    context = new JavaSparkContext(conf);
                }
            }
        }
        return context;
    }

    /**
     * Use of "first" returns the first element in the RDD
     * @param coloursList
     * @return
     */
    public String rddFirstDemo(List<String> coloursList) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<String> strJavaRDD = context.parallelize(coloursList);
        logger.info("============================== first ==============================");
        return strJavaRDD.first();
    }

    /**
     * Demo of "top" method on String based RDDs
     * @param coloursList
     */
    public void rddTopDemoOnStringRDD(List<String> coloursList) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<String> strJavaRDD = context.parallelize(coloursList);
        logger.info("============================== Top with Comparator.reverseOrder() ==============================");
        List<String> top1 = strJavaRDD.top(4, Comparator.reverseOrder());
        top1.forEach(logger::info);

        logger.info("============================== top with string comparator ==============================");
        List<String> top2 = strJavaRDD.top(4, (Comparator<String> & Serializable) (str1, str2) -> str1.compareTo(str2));
        top2.forEach(logger::info);
    }

    /**
     * Demo of "top" method on custom Object based RDDs
     * @param coursesList
     */
    public void rddTopDemoOnCustomObjectRDD(List<Course> coursesList) {
        JavaSparkContext context = getJavaSparkContext();
        JavaRDD<Course> coursesJavaRDD = context.parallelize(coursesList);
        logger.info("============================== top with nested comparator class ==============================");
        List<Course> topCourses1 = coursesJavaRDD.top(4, (Comparator<Course> & Serializable) (course1, course2) -> new CourseComparator().compare(course1, course2));
        topCourses1.stream().map(course -> course.courseName).forEach(logger::info);

        logger.info("========================= top with Comparator.comparing =========================");
        /*List<Course> topCourses2 = coursesJavaRDD.top(4, (Comparator<Course> & Serializable) Comparator.comparing(Course::getCourseName));
        topCourses2.stream().map(course -> course.courseName).forEach(logger::info);*/
    }

    static class Course implements Serializable {
        private String courseName;

        public Course() {
        }

        public Course(String courseName) {
            this.courseName = courseName;
        }

        public String getCourseName() {
            return courseName;
        }

        public void setCourseName(String courseName) {
            this.courseName = courseName;
        }
    }

    static class CourseComparator implements Comparator<Course>, Serializable {
        private static final long serialVersionUID = -112323232323222L;
        @Override
        public int compare(Course c1, Course c2) {
            return c1.courseName.compareTo(c2.courseName);
        }
    }


}
