package com.adrien.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    //不写会有空指针
    protected OrderGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //只要ID相同，认为相同的key
        int result;
        OrderBean aOrderBean = (OrderBean) a;
        OrderBean bOrderBean = (OrderBean) b;
        if (aOrderBean.getOrderId() > bOrderBean.getOrderId()) {
            result = 1;
        } else if (aOrderBean.getOrderId() < bOrderBean.getOrderId()) {
            result = -1;
        } else {
            result = 0;
        }
        return result;
    }
}
