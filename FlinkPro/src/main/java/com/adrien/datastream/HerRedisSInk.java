package com.adrien.datastream;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
//https://blog.csdn.net/qq_41311979/article/details/114852904


public class HerRedisSInk implements RedisMapper<SensorReading> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET,"sensor");
    }

    @Override
    public String getKeyFromData(SensorReading sensorReading) {
        return sensorReading.getId();
    }

    @Override
    public String getValueFromData(SensorReading sensorReading) {
        return sensorReading.getTemperature().toString();
    }
}
