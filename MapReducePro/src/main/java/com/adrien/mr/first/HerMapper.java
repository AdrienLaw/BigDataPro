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
 * @author luohaotian
 */
public class HerMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取一行数据
        String line = value.toString();
        //切分一行
        String[] split = line.split(",");
        //Text 对象 ，接收 split 遍历出来的数据，写入 context
        Text text = new Text();
        //给整型变量赋值 每一个计数1 ；若赋值为2 则计数2
        IntWritable intWritable = new IntWritable(2);
        for (String word : split) {
            text.set(word);
            context.write(text,intWritable);
        }
    }
}
