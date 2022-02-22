package com.adrien.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

import static java.lang.System.in;

/**
 * 重写 LineRecordReader :将 KV 转化为行号和行值
 */
public class HerRecordReader extends RecordReader<LongWritable, Text> {

    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private FSDataInputStream fileIn;
    private LongWritable key;
    private Text value;


    /***
     * 初始化的方法 仅在初始化的时候调用一次，只要获取到文件的切片，就拿到了文件的内容
     * @param inputSplit
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Path file = split.getPath();
        FileSystem fileSystem = file.getFileSystem(context.getConfiguration());
        fileIn = fileSystem.open(file);
        in = new LineReader(fileIn);
        start = split.getStart();
        end = start + split.getLength();
       pos = 1;
    }

    /**
     * 读取一行 将行号赋值给 key,将行值 赋值给 value
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        if (in.readLine(value) == 0) {
            return false;
        }
        pos ++;
        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
