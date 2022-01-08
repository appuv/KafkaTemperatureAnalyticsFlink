package com.appu.kafka.serde.serializer;

import com.appu.pojo.Equipment;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;

public class EquipmentValueSerializer  implements SerializationSchema<Equipment> {
    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }

    @Override
    public byte[] serialize(Equipment equipment) {
        return equipment.getValue().getBytes(StandardCharsets.UTF_8);
    }
}
