package com.testing.bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args){
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("StartingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

//        learning(sc);
//        pairRDD(sc);
//        flatMaps(sc);
//        readFromAFile(sc);
        readAndReturn(sc);
        sc.close();

    }

    private static void learning(JavaSparkContext sc){
        List<Integer> inputData = Arrays.asList(35, 12, 90, 20);

        JavaRDD<Integer> myRDD = sc.parallelize(inputData);

        Integer output = myRDD.reduce((value1, value2) -> value1+value2);

        JavaRDD<Tuple2<Integer, Double>> sqrtRDD = myRDD.map(f -> new Tuple2<>(f, Math.sqrt(f)));

//        Tuple2  myVal = new Tuple2(9, 3);

        sqrtRDD.collect().forEach(System.out::println);

        Long count = sqrtRDD.map(s -> 1L).reduce((v1, v2) -> v1+v2);

        System.out.println(count);

        System.out.println(output);
    }

    //Working with PiarRDD ::: Note: PiarRDD is a key value pair
    private static void pairRDD(JavaSparkContext sc){
        List<String> str = Arrays.asList("WARN: Tuesday 4 September 0405", "ERROR: Tuesday 4 September 0408",
                "FATAL: Wednesday 5 September 1632", "ERROR: Friday 7 September 1854",
                "WARN: Saturday 8 September 1942", "WARN: Tuesday 4 September 0406");

        JavaRDD<String> originalLog = sc.parallelize(str);

        JavaPairRDD<String, Long> pairRDD = originalLog.mapToPair(raw -> {
            String[] arr = raw.split(":");
            return new Tuple2<>(arr[0], 1L);
        });

        pairRDD.reduceByKey((value1, value2) -> value1+value2)
                .collect()
                .forEach(System.out::println);
    }

    //flatMaps
    private static void flatMaps(JavaSparkContext sc){
        List<String> str = Arrays.asList("WARN: Tuesday 4 September 0405", "ERROR: Tuesday 4 September 0408",
                "FATAL: Wednesday 5 September 1632", "ERROR: Friday 7 September 1854",
                "WARN: Saturday 8 September 1942", "WARN: Tuesday 4 September 0406");

        JavaRDD<String> originalLog = sc.parallelize(str);

        JavaRDD<String> result = originalLog.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

        result.collect().forEach(System.out::println);

        result.filter(t -> t.length() > 4).collect().forEach(System.out::println);
    }

    private static void readFromAFile(JavaSparkContext sc){
        JavaRDD<String> initiatedRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        initiatedRdd.flatMap(val -> Arrays.asList(val.split(" ")).iterator())
                .collect()
                .forEach(System.out::println);
    }

    private static void readAndReturn(JavaSparkContext sc){
        JavaRDD<String> initiatedRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        initiatedRdd.flatMap(val -> Arrays.asList(val.replace(",", "")
                        .replace(".", "").split(" ")).iterator())
                .filter(val -> val.length() > 0)
                .mapToPair(val -> new Tuple2<>(val, 1l))
                .reduceByKey((val1, val2)-> val1+val2)
                .mapToPair(val -> new Tuple2<>(val._2, val._1))
                .sortByKey(false)
                .take(10)
                .forEach(System.out::println);

    }
}
