package com.appu.kafka.serde.deserializer;

import com.appu.pojo.Equipment;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public class EquipmentRecordDeserializer implements KafkaRecordDeserializationSchema {


    @Override
    public void deserialize(ConsumerRecord consumerRecord, Collector collector) throws IOException {

        log.info("Key: {}", new String((byte[]) consumerRecord.key(), StandardCharsets.UTF_8));
        log.info("Value: {}", new String((byte[]) consumerRecord.value(), StandardCharsets.UTF_8));
        log.info("Offset: {}", consumerRecord.offset());
        log.info("Headers: {}", consumerRecord.headers());
        log.info("LeaderEpoch: {}", consumerRecord.leaderEpoch());
        log.info("Timestamp: {}", consumerRecord.timestamp());
        log.info("Partition: {}", consumerRecord.partition());
        log.info("Topic: {}", consumerRecord.topic());

        Equipment tempEquipment = new Equipment();
        tempEquipment.setKey(new String((byte[]) consumerRecord.key(), StandardCharsets.UTF_8));
        tempEquipment.setValue(new String((byte[]) consumerRecord.value(), StandardCharsets.UTF_8));

        collector.collect(tempEquipment);

    }

    @Override
    public TypeInformation getProducedType() {
        return null;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaRecordDeserializationSchema.super.open(context);
    }
}
