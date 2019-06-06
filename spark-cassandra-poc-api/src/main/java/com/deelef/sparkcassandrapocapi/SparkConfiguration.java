package com.deelef.sparkcassandrapocapi;

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

    @Bean
    public SparkConf conf() {
        return new SparkConf().setAppName(appName).setMaster(masterUri)
//                .set("spark.local.ip","10.0.75.1") // helps when multiple network interfaces are present. The driver must be in the same network as the master and slaves
//                .set("spark.driver.host","10.0.75.1") // same as above. This duality might disappear in a future version
                ;
    }

    @Bean
    public JavaSparkContext sc() {
        return new JavaSparkContext(conf());
    }

}
