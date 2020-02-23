package com.adrien.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class RemoveQuotesUDF extends UDF {
    public Text evaluate(Text str) {
        if (null == str) {
            return null;
        }
        if (StringUtils.isBlank(str.toString())) {
            return null;
        }
        return new Text(str.toString().replaceAll("\"",""));
    }

    public static void main(String[] args) {
        System.out.println(new RemoveQuotesUDF().evaluate(new Text("\"GET /course/view.php?id=27 HTTP/1.1\"")));
    }
}
