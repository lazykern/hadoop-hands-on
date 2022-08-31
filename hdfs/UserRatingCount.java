// For IO exception handling
import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Text;

public class UserRatingCount {
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private IntWritable rating = new IntWritable();
      private Text user_id = new Text();

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         String[] tokens = line.split("\t");
         user_id.set(tokens[0]);
         rating.set(Integer.parseInt(tokens[1]));
         context.write(user_id, rating);
      }
   }

   public static class Reduce extends Reducer<Text, IntWritable, Text, MapWritable> {

      public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
         MapWritable user_rating_count = new MapWritable();
         for (IntWritable val : values) {
            if (user_rating_count.containsKey(val)) {
               IntWritable count = (IntWritable) user_rating_count.get(val);
               count.set(count.get() + 1);
               user_rating_count.put(val, count);
            } else {
               user_rating_count.put(val, new IntWritable(1));
            }
         }
         context.write(key, user_rating_count);
      }
   }

   // Main method for the map reduce script
   public static void main(String[] args) throws Exception {
      // Create a new configuration (Empty)
      Configuration conf = new Configuration();

      // Create a new job instance
      Job job = Job.getInstance(conf, "UserRatingCount");

      // Set classes for the job
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(MapWritable.class);

      job.setMapperClass(Map.class);
      job.setCombinerClass(Reduce.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // Set the input and output paths for the job
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

}
