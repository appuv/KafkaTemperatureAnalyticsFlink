package com.appu.utils;

import com.appu.commons.Constants;
import com.appu.pojo.Equipment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;

public class analyzeTemperature implements MapFunction<Equipment, Equipment> {
    private ObjectMapper mapper ;

    @Override
    public Equipment map(Equipment equipment) throws Exception {
        mapper = new ObjectMapper();
        JsonNode inputJsonNode = mapper.readValue(equipment.getValue(),JsonNode.class);

        String temp_status = Constants.DEVICE_TEMPERATURE_INVALID;

        int temp = inputJsonNode.get(Constants.TEMPERATURE_KEY).asInt();

        //>=10  & <=30 = normal
        if(temp>=Constants.NORMAL_TEMP_FLOOR && temp<=Constants.NORMAL_TEMP_CEIL)
        {
            temp_status = Constants.DEVICE_TEMPERATURE_NORMAL;
        }
        // >30 & <=70 = hot
        else if(temp>Constants.NORMAL_TEMP_CEIL && temp<=Constants.INVALID_TEMP_CEIL)
        {
            temp_status = Constants.DEVICE_TEMPERATURE_HOT;
        }
        // <=-5 & < 10 is = cold
        else if(temp>=Constants.INVALID_TEMP_FLOOR && temp<Constants.NORMAL_TEMP_FLOOR)
        {
            temp_status = Constants.DEVICE_TEMPERATURE_COLD;
        }
        ((ObjectNode) inputJsonNode).put(Constants.DEVICE_TEMPERATURE_STATUS,temp_status);


        equipment.setValue(inputJsonNode.toString());
        return equipment;
    }
}
