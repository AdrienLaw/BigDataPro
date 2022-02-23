package com.adrien.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 可序列化 实现
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {

    @Override
    public int compareTo(FlowSortBean o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
