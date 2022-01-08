package com.appu.main;


import com.appu.commons.Commons;
import com.appu.kafka.streams.KafkaFlinkSourceSink;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EquipmentAnalytics {

    public static void main(final String... args) {

        log.info("Kafka Source Topic: {}", Commons.KAFKA_SOURCE_TOPIC);
        log.info("Kafka Destination Topic: {}", Commons.KAFKA_DESTINATION_TOPIC);
        log.info("Kafka  Bootstrap Server: {}", Commons.KAFKA_BOOTSTRAP_SERVER);

        //main function
        KafkaFlinkSourceSink kafkaFlinkSourceSink = new KafkaFlinkSourceSink();
        try {
            kafkaFlinkSourceSink.runAnalytics();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            log.error(e.getMessage());
        }


    }
}

