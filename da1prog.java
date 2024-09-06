package org.myorg;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class da1prog {

   // Mapper class
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
       private final static IntWritable one = new IntWritable(1);
       private Text country = new Text();

       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           // Skip the header line
           if (key.get() == 0 && value.toString().contains("date")) {
               return;
           }

           // Split CSV line into fields
           String[] fields = value.toString().split(",");
           if (fields.length >= 3) {
               // Extract home and away teams
               String homeTeam = fields[1];
               String awayTeam = fields[2];

               // Emit home team and away team as key with count 1
               country.set(homeTeam);
               context.write(country, one);

               country.set(awayTeam);
               context.write(country, one);
           }
       }
   }

   // Reducer class
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
       public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int sum = 0;
           for (IntWritable val : values) {
               sum += val.get();
           }
           context.write(key, new IntWritable(sum));
       }
   }

   // Main method to set up job configuration
   public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
       Job job = Job.getInstance(conf, "matchcount");
       job.setJarByClass(da1prog.class);
       job.setMapperClass(Map.class);
       job.setCombinerClass(Reduce.class);
       job.setReducerClass(Reduce.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
       System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
