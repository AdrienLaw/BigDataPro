package com.adrien.mr.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    private FlowBean flowBean;
    private Text text;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        flowBean = new FlowBean();
        text = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String phoneNum = fields[1];
        String upFlow = fields[6];
        String downFlow = fields[7];
        String upCountFlow = fields[8];
        String downCountFlow = fields[9];
        text.set(phoneNum);

        flowBean.setUpFlow(Integer.valueOf(upFlow));
        flowBean.setDownFlow(Integer.valueOf(downFlow));
        flowBean.setUpCountFlow(Integer.valueOf(upCountFlow));
        flowBean.setDownCountFlow(Integer.valueOf(downCountFlow));

        context.write(text,flowBean);
    }
}
