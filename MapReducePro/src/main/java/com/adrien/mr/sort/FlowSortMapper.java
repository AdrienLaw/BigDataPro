package com.adrien.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author luohaotian
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {
    private FlowSortBean flowSortBean;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        flowSortBean  = new FlowSortBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
         * 1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com	游戏娱乐	24	27	2481	24681	200
         * 1363157995052	13826544101	5C-0E-8B-C7-F1-E0:CMCC	120.197.40.4	jd.com	京东购物	4	0	264	0	200
         */
        String[] split = value.toString().split("\t");

        flowSortBean.setPhone(split[1]);
        flowSortBean.setUpPackNum(Integer.parseInt(split[6]));
        flowSortBean.setDownPackNum(Integer.parseInt(split[7]));
        flowSortBean.setUpPayload(Integer.parseInt(split[8]));
        flowSortBean.setDownPayload(Integer.parseInt(split[9]));
        
        context.write(flowSortBean,NullWritable.get());
    }
}
