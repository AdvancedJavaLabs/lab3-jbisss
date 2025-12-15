package com.itmo.ipkn;

import com.itmo.ipkn.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Stats {

    private static final int REDUCERS_FOR_SORT_JOB = 1;
    private static final long SPLIT_SIZE = 5485760;

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Job job1 = Job.getInstance(new Configuration(), "Category Revenue Quantity");
        job1.setJarByClass(Stats.class);

        job1.setMapperClass(RevenueQuantityMapper.class);
        job1.setReducerClass(RevenueQuantityReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // FileInputFormat.setMaxInputSplitSize(job1, SPLIT_SIZE);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        int numReducersJob = Integer.parseInt(args[4]);

        job1.setNumReduceTasks(numReducersJob);

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(new Configuration(), "Sort Categories");
        job2.setJarByClass(Stats.class);

        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setSortComparatorClass(DescendingTextComparator.class);

        // FileInputFormat.setMaxInputSplitSize(job2, SPLIT_SIZE);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        job2.setNumReduceTasks(REDUCERS_FOR_SORT_JOB);

        boolean waitForCompletion = job2.waitForCompletion(true);

        long end = System.currentTimeMillis();
        String text = "Total execution time for " + numReducersJob + " reducers: " + (end - start) + " ms";

        System.out.println("Total time: " + (end - start));

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("execution_time_output_" + numReducersJob + ".txt"))) {
            writer.write(text);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.exit(waitForCompletion ? 0 : 1);
    }
}
