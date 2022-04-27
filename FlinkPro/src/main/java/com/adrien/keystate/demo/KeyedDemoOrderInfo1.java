package com.adrien.keystate.demo;

import lombok.Data;

@Data
public class KeyedDemoOrderInfo1 {
    private Long orderId;
    private String produceName;
    private Double price;

    public static KeyedDemoOrderInfo1 string2OrderInfo1(String line){
        KeyedDemoOrderInfo1 orderInfo1 = new KeyedDemoOrderInfo1();
        if(line != null && line.length() > 0){
            String[] fields = line.split(",");
            orderInfo1.setOrderId(Long.parseLong(fields[0]));
            orderInfo1.setProduceName(fields[1]);
            orderInfo1.setPrice(Double.parseDouble(fields[2]));
        }
        return orderInfo1;
    }
}
