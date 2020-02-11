package com.adrien.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        /**
         * 	13568436656	 2481	24681	200
         * 	13568436656	 1116	954     200
         */
        //1. 累加求和
        long sun_upFlow = 0;
        long sun_downFlow = 0;
        for (FlowBean flowBean : values) {
            sun_upFlow += flowBean.getUpFlow();
            sun_downFlow += flowBean.getDownFlow();
        }

        flowBean.set(sun_upFlow,sun_downFlow);
        //2. 写出
        context.write(key,flowBean);
    }
}
