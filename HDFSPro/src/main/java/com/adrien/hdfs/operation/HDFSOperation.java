package com.adrien.hdfs.operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class HDFSOperation {

    @Test
    public void mkDir () throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mkdirs(new Path("/adrien/dir001"));
        fileSystem.close();
    }
}
