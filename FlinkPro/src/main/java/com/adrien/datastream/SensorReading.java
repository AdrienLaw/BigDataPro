package com.adrien.datastream;

import lombok.Data;

@Data
public class SensorReading {
    // 传感器 id
    private String id;
    // 传感器时间戳
    private Long timestamp;
    // 传感器温度
    private Double temperature;
}
