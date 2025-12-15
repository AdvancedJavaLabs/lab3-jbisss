package com.itmo.ipkn.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.math.BigDecimal;

public class RevenueQuantityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        BigDecimal totalRevenue = BigDecimal.ZERO;
        int totalQuantity = 0;

        for (Text v : values) {
            String[] parts = v.toString().split(",");
            double price = Double.parseDouble(parts[0]);
            int quantity = Integer.parseInt(parts[1]);

            totalRevenue = totalRevenue.add(BigDecimal.valueOf(price * quantity));
            totalQuantity += quantity;
        }

        context.write(new Text(key.toString().trim()),
                new Text(String.format(";%s;%d", totalRevenue.toPlainString(), totalQuantity)));
    }
}
