package com.testing.bigdata;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args){
        List<Integer> inputData = Arrays.asList(35, 12, 90, 20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("StartingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRDD = sc.parallelize(inputData);

        Integer output = myRDD.reduce((value1, value2) -> value1+value2);

        JavaRDD<Double> sqrtRDD = myRDD.map(f -> Math.sqrt(f));

        sqrtRDD.foreach(t -> System.out.println(t));

        System.out.println(output);

        sc.close();

    }
}
