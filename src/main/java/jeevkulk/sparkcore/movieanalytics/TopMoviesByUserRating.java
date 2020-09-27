package jeevkulk.sparkcore.movieanalytics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class TopMoviesByUserRating {

    public void processUsingRDD() {
        JavaSparkContext javaSparkContext = getJavaSparkContext();

        // UserID:int, ItemID:int, Rating:int, TimeStamp:int
        String filename = getQualifiedFilename("user_rating_for_movies.dat");
        JavaRDD<String> movieRatingRDD = javaSparkContext.textFile(filename);
        movieRatingRDD.top(10).forEach(System.out::println);

        JavaPairRDD<Integer, Integer> movieRatingPairedRDD = movieRatingRDD.mapToPair(str -> {
            String[] strArr = str.split("\t");
            return new Tuple2(Integer.parseInt(strArr[1]), Integer.parseInt(strArr[2]));
        });
        movieRatingPairedRDD.top(10, new TupleComparator1()).forEach(System.out::println);

        JavaPairRDD<Integer, Integer> aggregatedMovieRatingRDD = movieRatingPairedRDD.reduceByKey((x, y) -> x + y);
        aggregatedMovieRatingRDD.top(10, new TupleComparator1()).forEach(System.out::println);


		/*ItemID: int, MovieName:chararray, releasedate:chararray, empty:bytearray, IMDB_URL:bytearray, Genre_unknown:int,
		Genre_action:int, Genre_adventure:int, Genre_Animation:int, Genre_childrens:int, Genre_comedy:int,Genre_crime:int,
		Genre_documentary:int,Genre_drama:int,Genre_fantasy:int,Genre_FilmNoir:int,Genre_Horror:int,Genre_Musical:int,
		Genre_Mystery:int, Genre_Romance:int, Genre_SciFi:int,Genre_Thriller:int, Genre_War:int, Genre_Western:int*/
        filename = getQualifiedFilename("movies_info.dat");
        JavaRDD<String> movieInfoRDD = javaSparkContext.textFile(filename);
        JavaPairRDD<Integer, String> movieInfoPairedRDD = movieInfoRDD.mapToPair(str -> {
            String[] strArr = str.split(";");
            return new Tuple2<Integer, String>(Integer.parseInt(strArr[0]), strArr[1]);
        });
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedMovieRatingRDD = aggregatedMovieRatingRDD.join(movieInfoPairedRDD);

        List<Tuple2<Integer, Tuple2<Integer, String>>> topMoviesByRatingList = joinedMovieRatingRDD.top(5, new TupleComparator2());
        topMoviesByRatingList.forEach(System.out::println);
    }

    //TODO: Yet to complete
    public void processUsingDataset() {
        JavaSparkContext javaSparkContext = getJavaSparkContext();
        String filename = getQualifiedFilename("user_rating_for_movies.dat");
        JavaRDD<String> javaRDD = javaSparkContext.textFile(filename);

        // UserID:int, ItemID:int, Rating:int, TimeStamp:int
        javaRDD.top(10).forEach(System.out::println);
        JavaPairRDD<Integer, Integer> ratingRDD = javaRDD.mapToPair(str -> {
            String[] strArr = str.split("\t");
            return new Tuple2(Integer.parseInt(strArr[1]), Integer.parseInt(strArr[2]));
        });
        ratingRDD.top(10, new TupleComparator1()).forEach(System.out::println);
        JavaPairRDD<Integer, Integer> aggregatedRDD = ratingRDD.reduceByKey((x, y) -> x + y);
        aggregatedRDD.top(10, new TupleComparator1()).forEach(System.out::println);

        List<Tuple2<Integer, Integer>> topMoviesByRatingList = aggregatedRDD.top(5, new TupleComparator1());
        topMoviesByRatingList.forEach(System.out::println);
    }

    private String getQualifiedFilename(String filename) {
        return this.getClass().getClassLoader().getResource(filename).getFile();
    }

    private JavaSparkContext getJavaSparkContext() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("TopMoviesByUserRating")
                .setMaster("local[*]");
        return new JavaSparkContext(sparkConf);
    }

    public static void main(String[] args) {
        TopMoviesByUserRating topMoviesByUserRating = new TopMoviesByUserRating();
        topMoviesByUserRating.processUsingRDD();
    }

    public static class TupleComparator1 implements Comparator<Tuple2<Integer, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
            return t1._2 > t2._2 ? 1 : t1._2 < t2._2 ? -1 : 0;
        }
    }

    public static class TupleComparator2 implements Comparator<Tuple2<Integer, Tuple2<Integer, String>>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Tuple2<Integer, String>> t1, Tuple2<Integer, Tuple2<Integer, String>> t2) {
            return t1._2._1 > t2._2._1 ? 1 : t1._2._1 < t2._2._1 ? -1 : 0;
        }
    }
}
