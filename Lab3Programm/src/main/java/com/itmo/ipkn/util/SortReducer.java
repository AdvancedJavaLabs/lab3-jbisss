package com.itmo.ipkn.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {

    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //context.write(new Text("category;"), new Text("revenue;quantity"));
        for (Text v : values) {
            String[] p = v.toString().split(";");
            String cat = p[0];
            String rev = p[1];
            String qty = p[2];

            context.write(new Text(cat.trim()),
                    new Text(String.format(";%s;%s", rev, qty)));
        }
    }
}
