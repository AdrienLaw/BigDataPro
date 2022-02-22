package com.adrien.mr.partition;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class FlowMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(FlowMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://hadoop101:9000/adrien/input/flow/data_flow.dat"));

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setPartitionerClass(PartitionOwn.class);
        job.setNumReduceTasks(Integer.valueOf("3"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://hadoop101:9000/adrien/output/flow/"));

        boolean b = job.waitForCompletion(true);
    }
}
