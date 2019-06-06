package com.deelef.sparkcassandrapocapi;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfiguration {

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String masterUri;

    @Value("${cassandra.host}")
    String cassandraHost;

    @Value("${cassandra.keyspace}")
    String cassandraKeyspace;


    @Value("${docker.host.internal}")
    private String dockerHostInternal;

    @Bean
    public SparkConf conf() {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(masterUri);
        if(StringUtils.isNotBlank(dockerHostInternal)) {
            conf.set("spark.local.ip",dockerHostInternal);
            conf.set("spark.driver.host",dockerHostInternal);
        }


        conf.set("spark.cassandra.connection.host", dockerHostInternal);
        conf.set("spark.submit.deployMode", "client");

        return conf;

    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(conf());
    }


    @Bean
    public SparkSession sparksession() {
        SparkSession sp = new SparkSession(javaSparkContext().sc());
        return sp;
    }

}
