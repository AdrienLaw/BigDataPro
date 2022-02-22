package com.adrien.mr.inputformat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HerInputFormatReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
    FloatWritable avg = new FloatWritable(0);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        int n = 0;
        for (IntWritable value : values) {
            sum += value.get();
            n ++;
        }
        avg.set((float) (sum/(n*1.0)));
        context.write(key,avg);
    }
}
