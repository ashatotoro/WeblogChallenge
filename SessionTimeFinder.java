package com.asha.test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The class finds the latest visiting time for each user/ip in a session. 
 * Assume:
 *   - session time window is fixed, 15 minutes
 *   - this session time window starts at 2015-07-22T09:00:00.000Z
 *   - this session time windows ends at 2015-07-22T09:15:00.000Z
 * For example, if user1 visited some url at 2015-07-22T09:01:00.000Z and 2015-07-22T09:02:00.000Z,
 * the output for this user is:  (user1  2015-07-22T09:02:00.000Z).
 * It runs as a mapreduce job on a hadoop cluster.
 * Mapper prepares the data as (IP, Visiting time)
 * Reducer finds the most recent visiting time for each ip.
 * @author asha
 *
 */
public class SessionTimeFinder {

	public static class GetSessionTimeMapper extends Mapper<Object, Text, Text, Text> {

		private Text ip = new Text();
		private Text time = new Text();
		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] results = value.toString().split("\\s");
				String visitTime = results[0];
				String ipStr = results[2];
				Date startTime = sdf.parse("2015-07-22T09:00:00.000Z");
				Date endTime = sdf.parse("2015-07-22T09:15:00.000Z");
				String currentTimeStr = visitTime.substring(0, 20)
						+ String.valueOf(Integer.parseInt(visitTime.substring(20, 26)) / 1000) + "Z";
				Date currentTime = sdf.parse(currentTimeStr);
				if (currentTime.after(startTime) && currentTime.before(endTime)) {
					ip.set(ipStr);
					time.set(currentTimeStr);
					context.write(ip, time);
				}
			} catch (ParseException e) {
				System.out.println("Parsing error for the timestamp.");
			}
		}
	}

	public static class GetSessionTimeReducer extends Reducer<Text, Text, Text, Text> {
		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				Date startTime = sdf.parse("2015-07-22T09:00:00.000Z");
				Text maxTime = new Text("2015-07-22T09:00:00.000Z");
				for (Text val : values) {
					Date currentTime = sdf.parse(val.toString());
					if (currentTime.after(startTime)) {
						startTime = currentTime;
						maxTime = val;
					}
				}
				context.write(key, maxTime);
			} catch (ParseException e) {
				System.out.println("Parsing error for the timestamp.");
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "session time");
		job.setJarByClass(SessionTimeFinder.class);
		job.setMapperClass(GetSessionTimeMapper.class);
		job.setCombinerClass(GetSessionTimeReducer.class);
		job.setReducerClass(GetSessionTimeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
