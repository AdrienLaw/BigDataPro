package com.adrien.mr.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义 mapper 类需要继承 Mapper ，有四个泛型
 * keyin:
 * valuein:
 * keyout:
 * valueout:
 */
public class HerMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取一行数据
        String line = value.toString();
        String[] split = line.split(",");
        Text text = new Text();
        IntWritable intWritable = new IntWritable(1);
        for (String word : split) {
            text.set(word);
            context.write(text,intWritable);
        }
    }
}
