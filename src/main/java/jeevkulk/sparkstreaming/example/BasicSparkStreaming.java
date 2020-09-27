package jeevkulk.sparkstreaming.example;


import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class BasicSparkStreaming {

    public static void main(String[] args) {
        BasicSparkStreaming basicSparkStreaming = new BasicSparkStreaming();
        basicSparkStreaming.wordCount();
    }

    private void wordCount() {

        SparkConf conf = new SparkConf().setAppName("Streaming WordCount").setMaster("local[2]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("localhost", 9999);
        System.out.println(lines);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();

        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
