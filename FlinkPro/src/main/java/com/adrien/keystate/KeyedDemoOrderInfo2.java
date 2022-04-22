package com.adrien.keystate;

import lombok.Data;

@Data
public class KeyedDemoOrderInfo2 {
    private Long orderId;
    private String orderDate;
    private String address;


    public static KeyedDemoOrderInfo2 string2OrderInfo2(String line) {
        KeyedDemoOrderInfo2 orderInfo2 = new KeyedDemoOrderInfo2();
        if(line != null && line.length() > 0){
            String[] fields = line.split(",");
            orderInfo2.setOrderId(Long.parseLong(fields[0]));
            orderInfo2.setOrderDate(fields[1]); orderInfo2.setAddress(fields[2]);
        }
        return orderInfo2;
    }
}
