package com.adrien.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.FileOutputStream;

public class HerInputFormatMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "local");
        Path outPut = new Path("file:///D:/out");
        FileSystem fileSystem = outPut.getFileSystem(configuration);
        if (fileSystem.exists(outPut)) {
            fileSystem.delete(outPut,true);
        }
        Job job = Job.getInstance(configuration);
        job.setJobName("ageAvg");
        job.setJarByClass(HerInputFormatMain.class);
        job.setMapperClass(HerInputFormatMapper.class);
        job.setReducerClass(HerInputFormatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setInputFormatClass(HerInputFormat.class);
        FileInputFormat.addInputPath(job,new Path(""));
        FileOutputFormat.setOutputPath(job,outPut);
        System.exit(job.waitForCompletion(true) ? 0:1);
        return 0;
    }
}
