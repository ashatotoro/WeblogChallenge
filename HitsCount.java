package com.asha.test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * This class aggregates the number of the visits for each user. (goal 1 in the task list)
 * Assume:
 *   - session time window is fixed, 15 minutes
 *   - this session time window starts at 2015-07-22T09:00:00.000Z
 *   - this session time windows ends at 2015-07-22T09:15:00.000Z
 * It runs as a mapreduce job on a hadoop cluster.
 * Mapper prepares the data as (IP, 1)
 * Reducer adds the number up for each IP.
 * @author asha
 *
 */
public class HitsCount {

	public static class HitsCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text ip = new Text();
		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Date startTime;
			try {
				String[] results = value.toString().split("\\s");
				String visitTime = results[0];
				String ipStr = results[2];
				startTime = sdf.parse("2015-07-22T09:00:00.000Z");
				Date endTime = sdf.parse("2015-07-22T09:15:00.000Z");
				String currentTimeStr = results[0].substring(0, 20)
						+ String.valueOf(Integer.parseInt(visitTime.substring(20, 26)) / 1000) + "Z";
				Date currentTime = sdf.parse(currentTimeStr);
				if (currentTime.after(startTime) && currentTime.before(endTime)) {
					ip.set(ipStr);
					context.write(ip, one);
				}
			} catch (ParseException e) {
				System.out.println("Parsing error for the timestamp.");
			}
		}
	}

	public static class HitsCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "hits count");
		job.setJarByClass(HitsCount.class);
		job.setMapperClass(HitsCountMapper.class);
		job.setCombinerClass(HitsCountReducer.class);
		job.setReducerClass(HitsCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
