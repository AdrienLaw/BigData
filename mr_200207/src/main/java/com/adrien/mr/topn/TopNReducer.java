package com.adrien.mr.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class TopNReducer extends Reducer<FlowNBean, Text, Text, FlowNBean> {

    // 定义一个TreeMap作为存储数据的容器（天然按key排序）
    TreeMap<FlowNBean, Text> flowMap = new TreeMap<FlowNBean, Text>();

    @Override
    protected void reduce(FlowNBean key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

        for (Text value : values) {

            FlowNBean bean = new FlowNBean();
            bean.set(key.getDownFlow(), key.getUpFlow());

            // 1 向treeMap集合中添加数据
            flowMap.put(bean, new Text(value));

            // 2 限制TreeMap数据量，超过10条就删除掉流量最小的一条数据
            if (flowMap.size() > 10) {
                // flowMap.remove(flowMap.firstKey());
                flowMap.remove(flowMap.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Reducer<FlowNBean, Text, Text, FlowNBean>.Context context) throws IOException, InterruptedException {

        // 3 遍历集合，输出数据
        Iterator<FlowNBean> it = flowMap.keySet().iterator();

        while (it.hasNext()) {

            FlowNBean v = it.next();

            context.write(new Text(flowMap.get(v)), v);
        }
    }
}