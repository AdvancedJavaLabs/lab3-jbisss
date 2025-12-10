package com.itmo.ipkn.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class RevenueQuantityMapper extends Mapper<Object, Text, Text, Text> {
    private final Text category = new Text();
    private final Text valueOut = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("transaction_id")) return;

        String[] parts = value.toString().split(",");
        if (parts.length < 5) return;

        String cat = parts[2].trim();
        double price = Double.parseDouble(parts[3].trim());
        int quantity = Integer.parseInt(parts[4].trim());

        category.set(cat);
        valueOut.set(price + "," + quantity);
        context.write(category, valueOut);
    }
}
