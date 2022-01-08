package com.appu.commons;

public class Commons {
    
    public final static String KAFKA_SOURCE_TOPIC = System.getenv("KAFKA_SOURCE_TOPIC") != null ?
            System.getenv("KAFKA_SOURCE_TOPIC") : "flink_source";
    public final static String KAFKA_DESTINATION_TOPIC = System.getenv("KAFKA_DESTINATION_TOPIC") != null ?
            System.getenv("KAFKA_DESTINATION_TOPIC") : "flink_destination";

    public final static String KAFKA_BOOTSTRAP_SERVER = System.getenv("KAFKA_BOOTSTRAP_SERVER") != null ?
            System.getenv("KAFKA_BOOTSTRAP_SERVER") : "broker:29092";

}
