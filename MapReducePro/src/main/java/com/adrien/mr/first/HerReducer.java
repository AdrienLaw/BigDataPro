package com.adrien.mr.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HerReducer extends Reducer<Text, IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int result = 0;
        for (IntWritable value : values) {
            result += value.get();

        }
        IntWritable intWritable = new IntWritable(result);
        context.write(key,intWritable);
    }
}
