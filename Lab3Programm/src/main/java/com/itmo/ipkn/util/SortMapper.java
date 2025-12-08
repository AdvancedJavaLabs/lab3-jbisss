package com.itmo.ipkn.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {

    private final DoubleWritable revenue = new DoubleWritable();
    private final Text category = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("products")) return;
        String[] parts = value.toString().trim().split(";");
        String cat = parts[0];
        double rev = Double.parseDouble(parts[1]);
        int qty = Integer.parseInt(parts[2]);

        revenue.set(-rev); // для сортировки по убыванию
        category.set(cat.trim() + ";" + rev + ";" + qty);

        context.write(revenue, category);
    }
}
