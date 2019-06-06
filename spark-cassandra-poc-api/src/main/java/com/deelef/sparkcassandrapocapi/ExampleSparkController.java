package com.deelef.sparkcassandrapocapi;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RequestMapping("/api/rest")
@RestController
public class ExampleSparkController {


    private final JavaSparkContext sc;

    @Autowired
    public ExampleSparkController(JavaSparkContext sc) {
        this.sc = sc;
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
        JavaRDD<String> words = sc.parallelize(wordList);
        Map<String, Long> wordCounts = words.countByValue();
        return wordCounts;
    }

}
