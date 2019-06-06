package com.deelef.sparkcassandrapocapi;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

@RequestMapping("/api/rest")
@RestController
public class ExampleSparkController {


    private final JavaSparkContext sparkContext;
    private final SparkSession sparkSession;

    @Autowired
    public ExampleSparkController(JavaSparkContext sparkContext, SparkSession sparkSession) {
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
    }

    @RequestMapping(method = RequestMethod.GET, path = "/hello")
    public String hello() {
        return "hello";
    }

    @RequestMapping(method = RequestMethod.GET, path = "/wordcount")
    public Map<String, Long> count(@RequestParam(required = true) String words) {
        List<String> wordList = Arrays.asList(words.split("\\|"));
        return getCount(wordList);
    }

    private Map<String, Long> getCount(List<String> wordList) {
        JavaRDD<String> words = sparkContext.parallelize(wordList);
        Map<String, Long> wordCounts = words.countByValue();
        return wordCounts;
    }


    @RequestMapping(method = RequestMethod.GET, path = "/people")
    public List<Person> list() {
        Dataset<Row> peakset = sparkSession.read().format("org.apache.spark.sql.cassandra")
                .options(new HashMap<String, String>() {
                    {
                        put("keyspace", "example");
                        put("table", "person");
                    }
                })
                .load().select(col("id"));
        System.out.println(peakset.collectAsList());
        return Arrays.asList();
    }

}
