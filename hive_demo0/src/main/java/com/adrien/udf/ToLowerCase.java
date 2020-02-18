package com.adrien.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ToLowerCase extends UDF {
    public Text evaluate(Text text) {
        if (text == null) {
            return null;
        }
        if (text != null && text.toString().length() <= 0) {
            return null;
        }

        return new Text(text.toString().toLowerCase());
    }

    public static void main(String[] args) {
        System.out.println(new ToLowerCase().evaluate(new Text(args [0])));
    }
}
