package com.eecs476;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.util.*;

public class AssociationRules {
    private static int s;
    private static int k;

    // mapper for pass 1
    public static class AssociationMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            // value = word word word
            String[] substrings = value.toString().split(" ");
            for(int i = 0; i < substrings.length; ++i) {
                // key = word
                // value = 1
                context.write(new Text(substrings[i]),one);
            }
        }
    }

    // reducer for pass one
    public static class AssociationReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        // calculate support for each movieID
        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int minSupport = context.getConfiguration().getInt("minSupport",1);
            for(IntWritable value: values) {
                sum += value.get();
            }
            // if support >= threshold
            if (sum >= minSupport) {
                // key = movieID, value = support
                context.write(key, new IntWritable(sum));
            }
        }
    }
    // Reducer for K >= 2
    public static class AssociationReducerK
            extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            // key = movieID1,movieID2
            // value = sup(movie1),sup(movie2)
            int minSupport = context.getConfiguration().getInt("minSupport",1);

            int sum = 0; // support for pair(movie1,movie2)
            String cur_support = ""; // "sup(movie1),sup(movie2)"
            for(Text value: values) {
                sum += 1;
                cur_support = value.toString();
            }

            if (sum >= minSupport) {
                // support for movie1 and movie2
                String[] cur_sur_num = cur_support.split(",");
                // key = movieID1,movieID2
                String[] key_str = key.toString().split(",");

                // calculate conf movie1->movie2
                double con1 = sum / Double.parseDouble(cur_sur_num[0]);
                // calculate conf movie2->movie1
                double con2 = sum / Double.parseDouble(cur_sur_num[1]);

                context.write(new Text(key_str[0]+"->"+key_str[1]), new DoubleWritable(con1));
                context.write(new Text(key_str[1]+"->"+key_str[0]), new DoubleWritable(con2));

            }
        }
    }

    public static class AssociationMapperK
            extends Mapper<LongWritable, Text, Text, Text> {
        // movieID -> support
        private Map<String, String> prevItem = new HashMap<String, String>();
        private List<List<String>> candidateItem = new ArrayList<List<String>>();

        @Override
        public void setup(Context context)
                throws IOException {
            String outputPrefix = context.getConfiguration().get("outputPath", "");
            // get current passNum
            int passNum = context.getConfiguration().getInt("passNum", 2);
            // last output file directory
            String lastpass = outputPrefix + (passNum - 1) + "/part-r-00000";
            // read the file
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(lastpass))));
            // start to read file line by line
            String line;
            int idx = 0;
            while ((line = reader.readLine()) != null) {
                if (idx != 0) {
                    // line = movieID,support
                    String[] substrings = line.split(",");
                    // movieID -> support (MAP)
                    prevItem.put(substrings[0], substrings[1]);
                }
                idx += 1;
            }
            List<String> prev_list = new ArrayList<String>(prevItem.keySet());
            // Generate all possible subsets of prevItems
            candidateItem = Combination.combinator(prev_list, passNum);
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            // value = word word word
            String[] substrings = value.toString().split(" ");
            // all the movies the user has watched
            Set<String> movieIDs = new HashSet<String>();
            for (int i = 0; i < substrings.length; ++i) {
                movieIDs.add(substrings[i]);
            }

            for (List<String> candidate : candidateItem) {
                // check if {1,2} is a subset of {1,2,3,4,5}
                if (movieIDs.containsAll(candidate)) {
                    String output = "";
                    String value_res = "";
                    for (int i = 0; i < candidate.size(); ++i) {
                        output += candidate.get(i) + ","; // key=id1,id2
                        value_res += prevItem.get(candidate.get(i)) + ","; // value=support1,support2
                    }
                    // get rid of the last comma
                    output = output.substring(0, output.length() - 1);
                    value_res = value_res.substring(0, value_res.length() - 1);

                    context.write(new Text(output), new Text(value_res));
                }
            }
        }
    }

    static void runJobs(int passNum, String inputpath, String outputpath
    ) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "eecs476");         // required for this to work on GreatLakes
        conf.set("mapred.textoutputformat.separator", ",");
        conf.setInt("passNum", passNum);
        conf.setInt("minSupport", s);
        conf.set("outputPath", outputpath);
        Job job = new Job(conf, "job" + passNum);
        job.setJarByClass(AssociationRules.class);

        if (passNum == 1) {
            job.setMapperClass(AssociationMapper.class);
            job.setReducerClass(AssociationReducer.class);
            // set mapper output key and value class
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // set reducer output key and value class
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

        } else {
            job.setMapperClass(AssociationMapperK.class);
            job.setReducerClass(AssociationReducerK.class);
            // set mapper output key and value class
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // set reducer output key and value class
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        FileInputFormat.addInputPath(job, new Path(inputpath));
        FileOutputFormat.setOutputPath(job, new Path(outputpath + passNum));

        job.waitForCompletion(true);
    }

    public static void main(String[] args
    ) throws InterruptedException, IOException, ClassNotFoundException{
        String inpath = "/Users/claudiaz/Desktop/476FinalProject/";
        String outpath = "/Users/claudiaz/Desktop/476FinalProject/";
        String[] categories = new String[]{"Beauty & Spas","Home Services","Food","Restaurants", "Shopping"};
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("-k")) {
                k = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-s")) {
                s = Integer.parseInt(args[++i]);
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }
        for(String category: categories) {
            String inputpath = inpath + category + ".txt";
            String outputpath = outpath + category;
            for (int i = 1; i <= k; ++i) {
                runJobs(i, inputpath, outputpath);
            }

        }



    }
}

