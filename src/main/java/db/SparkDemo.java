package db;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class SparkDemo {

    public void doProcess() {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Passing Criteria")
                .setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //name: String, subject: String, marks: int
        String studentsMarksFilename = this.getClass().getClassLoader().getResource("students_marks.csv").getFile();
        JavaRDD<String> studentsMarksRDD = sparkContext.textFile(studentsMarksFilename);
        JavaPairRDD<String, Tuple2<String, Integer>> studentsMarksPairRDD = studentsMarksRDD.mapToPair(str -> {
            String[] strArr = str.split(",");
            return new Tuple2<String, Tuple2<String, Integer>>(strArr[1], new Tuple2<>(strArr[0], Integer.parseInt(strArr[2])));
        });
        studentsMarksPairRDD.top(10, new TupleComparator1()).forEach(System.out::println);

        //subject: String, passing_marks:Integer
        String subjectSuccessCriteriaFilename = this.getClass().getClassLoader().getResource("subject_success_criteria.csv").getFile();
        JavaRDD<String> passingMarksRDD = sparkContext.textFile(subjectSuccessCriteriaFilename);
        JavaPairRDD<String, Integer> passingMarksPairRDD = passingMarksRDD.mapToPair(str -> {
            String[] strArr = str.split(",");
            return new Tuple2<String, Integer>(strArr[0], Integer.parseInt(strArr[1]));
        });
        passingMarksPairRDD.top(10, new TupleComparator2()).forEach(System.out::println);

        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> joinedRDD = studentsMarksPairRDD.join(passingMarksPairRDD);
        joinedRDD.top(10, new TupleComparator3()).forEach(System.out::println);
        JavaRDD<Tuple2<Tuple2<String, String>, String>> javardd = joinedRDD.map(data -> {
            Tuple2<Tuple2<String, String>, String> tuple2;
            if (data._2._1._2 > data._2._2) {
                tuple2 = new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(data._1, data._2._1._1), "Pass");
            } else {
                tuple2 = new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(data._1, data._2._1._1), "Fail");
            }
            return tuple2;
        });
        javardd.top(10, new Comparator4()).forEach(System.out::println);
    }

    public static void main(String[] args) {
        SparkDemo sparkDemo = new SparkDemo();
        sparkDemo.doProcess();
    }

    public static class TupleComparator1 implements Comparator<Tuple2<String, Tuple2<String, Integer>>>, Serializable {
        @Override
        public int compare(Tuple2<String, Tuple2<String, Integer>> t1, Tuple2<String, Tuple2<String, Integer>> t2) {
            return t1._1.compareTo(t2._1);
        }
    }

    public static class TupleComparator2 implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t1._1.compareTo(t2._1);
        }
    }

    public static class TupleComparator3 implements Comparator<Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>>>, Serializable {
        @Override
        public int compare(Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> t1, Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> t2) {
            return t1._1.compareTo(t2._1);
        }
    }

    public static class Comparator4 implements Comparator<Tuple2<Tuple2<String, String>, String>>, Serializable {
        @Override
        public int compare(Tuple2<Tuple2<String, String>, String> t1, Tuple2<Tuple2<String, String>, String> t2) {
            return t1._1._1.compareTo(t2._1._1);
        }
    }
}
