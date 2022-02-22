package com.adrien.mr.inputformat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HerInputFormatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text ji = new Text("奇数");
    Text ou = new Text("偶数");
    IntWritable age = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        age.set(Integer.valueOf(value.toString()));
        if (key.get() % 2 == 1) {
            context.write(ji,age);
        } else if (key.get() % 2 == 0) {
            context.write(ou,age);
        }

    }
}
