package com.deelef.sparkcassandrapocapi;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfiguration {

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String masterUri;

    @Value("${docker.host.internal}")
    private String dockerHostInternal;

    @Bean
    public SparkConf conf() {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(masterUri);
        if(StringUtils.isNotBlank(dockerHostInternal)) {
            conf.set("spark.local.ip",dockerHostInternal);
            conf.set("spark.driver.host",dockerHostInternal);
        }
        return conf;

    }

    @Bean
    public JavaSparkContext sc() {
        return new JavaSparkContext(conf());
    }

}
