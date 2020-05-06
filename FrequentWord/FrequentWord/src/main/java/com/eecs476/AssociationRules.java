package com.eecs476;

import jdk.nashorn.internal.parser.JSONParser;
import net.minidev.json.JSONArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
            extends Mapper<LongWritable, Text, Text,IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value = reviews
            String[] substrings = value.toString().split(" ");
            for(int i = 0; i < substrings.length; ++i) {
                context.write(new Text(substrings[i]),one);
            }
        }
    }

    public static class AssociationRulesReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String reviewInput = "/Users/claudiaz/Desktop/476FinalProject/Shopping.txt";
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
