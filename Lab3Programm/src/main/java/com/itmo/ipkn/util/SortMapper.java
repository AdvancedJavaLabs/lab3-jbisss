package com.itmo.ipkn.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, Text, Text> {

    private final Text revenue = new Text();
    private final Text category = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("products")) return;
        String[] parts = value.toString().trim().split(";");
        String cat = parts[0];
        String rev = parts[1];
        int qty = Integer.parseInt(parts[2]);

        System.out.println("!!!!!!!!!" + rev);
        revenue.set(rev);
        category.set(cat.trim() + ";" + rev + ";" + qty);

        context.write(revenue, category);
    }
}
