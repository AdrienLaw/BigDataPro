package com.adrien.mr.sort;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 可序列化 实现
 * @author luohaotian
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {
    private String phone;
    private Integer upPackNum;
    private Integer downPackNum;
    private Integer upPayload;
    private Integer downPayload;

    /**
     * 比较两个 FlowSortBean 对象
     * 先对下行流量升序排序，相同按照上行流量进行降序排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowSortBean o) {
        //升序
        int i = this.downPackNum.compareTo(o.downPackNum);
        if (i == 0) {
            i = -this.upPayload.compareTo(o.upPayload);
        }
        return i;
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeInt(upPackNum);
        out.writeInt(downPackNum);
        out.writeInt(upPayload);
        out.writeInt(downPayload);


    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.upPackNum = in.readInt();
        this.downPackNum = in.readInt();
        this.upPayload = in.readInt();
        this.downPayload = in.readInt();
    }


    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Integer getUpPackNum() {
        return upPackNum;
    }

    public void setUpPackNum(Integer upPackNum) {
        this.upPackNum = upPackNum;
    }

    public Integer getDownPackNum() {
        return downPackNum;
    }

    public void setDownPackNum(Integer downPackNum) {
        this.downPackNum = downPackNum;
    }

    public Integer getUpPayload() {
        return upPayload;
    }

    public void setUpPayload(Integer upPayload) {
        this.upPayload = upPayload;
    }

    public Integer getDownPayload() {
        return downPayload;
    }

    public void setDownPayload(Integer downPayload) {
        this.downPayload = downPayload;
    }

    @Override
    public String toString() {
        return "FlowSortBean{" +
                "phone='" + phone + '\'' +
                ", upPackNum=" + upPackNum +
                ", downPackNum=" + downPackNum +
                ", upPayload=" + upPayload +
                ", downPayload=" + downPayload +
                '}';
    }
}
