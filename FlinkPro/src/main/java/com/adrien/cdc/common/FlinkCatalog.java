package com.adrien.cdc.common;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkCatalog {

    static String hiveDatabase= "traffic";
    static String catalogName = "traffic";
    public static void setHiveCatalog(StreamTableEnvironment tableEnvironment )  {


        String hiveConfDir = FlinkCatalog.class.getClassLoader().getResource("conf").getPath();

        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, hiveDatabase, hiveConfDir);
        tableEnvironment.registerCatalog(catalogName,hiveCatalog);
        tableEnvironment.useCatalog(catalogName);
    }
}
