package com.appu.kafka.streams;


import com.appu.commons.Commons;
import com.appu.kafka.serde.deserializer.EquipmentRecordDeserializer;
import com.appu.kafka.serde.serializer.EquipmentKeySerializer;
import com.appu.kafka.serde.serializer.EquipmentValueSerializer;
import com.appu.pojo.Equipment;
import com.appu.utils.analyzeTemperature;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

@Slf4j
public class KafkaFlinkSourceSink {

    public void runAnalytics() throws  Exception {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Properties
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.KAFKA_BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "EquipmentAnalytics003");

        //input data : {"serial" : "1"}|{"serial":"1","owner":"appu","temp":"70","location":"earth"}
        //Equipment Kafka Source
        KafkaSource<Equipment> equipmentKafkaSource = KafkaSource.<Equipment>builder()
                .setBootstrapServers(Commons.KAFKA_BOOTSTRAP_SERVER)
                .setTopics(Commons.KAFKA_SOURCE_TOPIC)
                .setGroupId("EquipmentAnalytics003")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new EquipmentRecordDeserializer())
                .build();

        //Equipment Kafka Destination
        KafkaSink<Equipment> equipmentKafkaSink = KafkaSink.<Equipment>builder()
                .setBootstrapServers(Commons.KAFKA_BOOTSTRAP_SERVER)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(Commons.KAFKA_DESTINATION_TOPIC)
                        .setKeySerializationSchema(new EquipmentKeySerializer())
                        .setValueSerializationSchema(new EquipmentValueSerializer())
                        .build()
                ).setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        TypeInformation<Equipment> typeInformation = TypeInformation.of(Equipment.class);

        DataStream<Equipment> inputDataStream = env.fromSource(equipmentKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Equipment Kafka Source",
                typeInformation);


        //temperature analytics

        inputDataStream.map(new analyzeTemperature())
                .sinkTo(equipmentKafkaSink).name("Equipment Kafka Destination");

        env.execute();


    }
}
