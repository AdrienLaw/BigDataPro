package com.adrien.mr.first;

import com.google.inject.internal.cglib.core.$DuplicatesPredicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.FileOutputStream;
import java.util.Properties;

public class HerWordCount {
    public static void main(String[] args) {
        //解决 permission 问题
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME","root");
        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://hadoop101:9000");
        config.set("yarn.resourcemanager.hostname", "hadoop104");
        try {
            FileSystem fileSystem = FileSystem.get(config);
            Job job = Job.getInstance();
            job.setJarByClass(HerWordCount.class);
            job.setJobName("wc");
            job.setMapperClass(HerMapper.class);
            job.setReducerClass(HerReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job,new Path("hdfs://hadoop101:9000/adrien/input/inputher"));
            Path outPath = new Path("hdfs://hadoop101:9000/adrien/outputher");
            if (fileSystem.exists(outPath)) {
                fileSystem.delete(outPath,true);
            }
            FileOutputFormat.setOutputPath(job,outPath);
            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("=  欧了 =");
            }
        } catch (Exception e) {

        }
    }


}
