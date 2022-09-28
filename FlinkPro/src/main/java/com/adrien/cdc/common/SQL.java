package com.adrien.cdc.common;

public class SQL {

    final static String taxi_gps_bcdc_drop = "DROP TABLE IF EXITS taxi_gps_cdc";
    final static String taxi_gps_bcdc_create =  "create table taxi_gps_cdc\n" +
            "(\n" +
            "    taxi_id        STRING,\n" +
            "    gps_dataTime   STRING,\n" +
            "    gps_longitude  STRING,\n" +
            "    gps_latitude   STRING,\n" +
            "    taxi_speed     STRING,\n" +
            "    taxi_direction STRING,\n" +
            "    taxi_status    STRING\n" +
            ") WITH (\n" +
            "    'connector' = 'mysql-cdc',\n" +
            "    'hostname'='hadoop101',\n" +
            "    'port'='3306',\n" +
            "    'username'='root',\n" +
            "    'password'='up15321370403',\n" +
            "    'database-name'='aikfk',\n" +
            "    'table-name'='user_info',\n" +
            ")";


}
