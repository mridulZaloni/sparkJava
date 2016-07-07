package com.zaloni.mgohain.word_count;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by mgohain on 7/7/2016.
 */
public class WordCount {
    public static void main (String [] args) {
        if (args.length !=2 ) {
            System.out.println("In correct number of inputs." + args.length + "\nProvide correct number of inputs.");
            System.exit(1);
        }
        String input = args[0];
        String output = args[1];
        SparkConf conf = new SparkConf().setAppName("Word Count");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        //Loading input data
        JavaRDD<String> lines = javaSparkContext.textFile(input);
        /**
         * MAP PHASE
         * Splitting the lines into words
         * FlatMapFunction<String, String>() -- here first String is the argument type and second String is the return type
         * Each line will get split at ","
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        });
        /**
         * Mapping the words with count 1 eg. (word, 1)
         * PairFunction<String, String, Integer>() -- First String is the argument type and <second(key), third(Value)> Strings are the type of the tuple to be returned
         */
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1) ;
            }
        });
        /**
         * REDUCE PHASE
         * Getting the total counts of the words eg (word,12)
         * Function2<Integer, Integer, Integer>() -- First and second Integer are the argument types and third integer is the return type
         */
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) throws Exception {
                return (a + b);
            }
        });
        //Saving the result
        counts.saveAsTextFile(output);
    }
}
