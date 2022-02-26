package com.adrien.mr.combine;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author luohaotian
 */
public class MyGroup extends WritableComparator {
    public MyGroup() {
        //分组类：对 OrderBean 类型的key 进行分组
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean a1 = (OrderBean) a;
        OrderBean b1 = (OrderBean) b;
        //将同一订单的kv 作为一组
        return a1.getOrderId().compareTo(b1.getOrderId());
    }
}
