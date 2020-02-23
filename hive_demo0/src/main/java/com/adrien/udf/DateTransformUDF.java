package com.adrien.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;


public class DateTransformUDF extends UDF {

    /**
     * 31/Aug/2015:00:04:37 +0800
     */
    private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yy:HH:mm:ss", Locale.ENGLISH);
    private final SimpleDateFormat onputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Text evaluate(Text str) {
        Text output = new Text();
        if (null == str) {
            return null;
        }
        if (StringUtils.isBlank(str.toString())){
            return null;
        }
        Date parseDate = null;
        try {
            parseDate = inputFormat.parse(str.toString().trim());
            String outputDate = onputFormat.format(parseDate);
            output.set(outputDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return output;
    }

    public static void main(String[] args) {
        Text evaluate = new DateTransformUDF().evaluate(new Text("31/Aug/2015:00:04:37 +0800"));
        System.out.println(evaluate.toString());
    }
}
