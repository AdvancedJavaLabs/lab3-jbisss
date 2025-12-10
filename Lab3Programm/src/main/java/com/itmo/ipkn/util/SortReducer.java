package com.itmo.ipkn.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class SortReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text v : values) {
            String[] p = v.toString().split(";");
            String cat = p[0];
            String rev = p[1];
            String qty = p[2];

            BigDecimal revenueAsBigDecimal = new BigDecimal(rev)
                    .setScale(4, RoundingMode.HALF_UP);

            context.write(new Text(String.format("%20s", cat.trim())),
                    new Text(String.format(";%20s;%20s;", revenueAsBigDecimal.toPlainString(), qty)));
        }
    }
}
