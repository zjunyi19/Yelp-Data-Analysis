package com.eecs476;

import jdk.nashorn.internal.parser.JSONParser;
import net.minidev.json.JSONArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class AssociationRules {

    public static class AssociationRulesMapper
            extends Mapper<LongWritable, Text, Text,DoubleWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value = category, rating
            String[] substrings = value.toString().split(",");
            context.write(new Text(substrings[0]), new DoubleWritable(Double.parseDouble(substrings[1])));

        }
    }

    public static class AssociationRulesReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            for(DoubleWritable value: values) {
                sum += value.get();
                count += 1;
            }
            context.write(key, new DoubleWritable(sum/count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String reviewInput = "/Users/claudiaz/Desktop/476FinalProject/category_star.csv";
        String outputpath = "/Users/claudiaz/Desktop/476FinalProject/output";
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "eecs476");         // required for this to work on GreatLakes
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = new Job(conf, "job");
        job.setJarByClass(AssociationRules.class);
        job.setMapperClass(AssociationRulesMapper.class);
        job.setReducerClass(AssociationRulesReducer.class);
        // set mapper output key and value class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // set reducer output key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(reviewInput));
        FileOutputFormat.setOutputPath(job, new Path(outputpath));

        job.waitForCompletion(true);
    }
}
