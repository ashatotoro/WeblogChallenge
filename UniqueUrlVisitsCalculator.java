package com.asha.test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

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
 * This class aggregates the unique visits for each URL in a session (Goal 3 in the task list)
 * Assume:
 *   - session time window is fixed, 15 minutes
 *   - this session time window starts at 2015-07-22T09:00:00.000Z
 *   - this session time windows ends at 2015-07-22T09:15:00.000Z
 * It runs as a mapreduce job on a hadoop cluster.
 * Mapper prepares the data as (url, ip)
 * Reducer removes the duplicated ips and count the unique visits.
 * @author asha
 *
 */
public class UniqueUrlVisitsCalculator {

	public static class UniqueUrlMapper extends Mapper<Object, Text, Text, Text> {

		private Text ip = new Text();
		private Text url = new Text();
		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] results = value.toString().split("\\s");
				String visitTime = results[0];
				String ipStr = results[2];
				String urlStr = results[12];
				Date startTime = sdf.parse("2015-07-22T09:00:00.000Z");
				Date endTime = sdf.parse("2015-07-22T09:15:00.000Z");
				String currentTimeStr = visitTime.substring(0, 20)
						+ String.valueOf(Integer.parseInt(visitTime.substring(20, 26)) / 1000) + "Z";
				Date currentTime = sdf.parse(currentTimeStr);
				if (currentTime.after(startTime) && currentTime.before(endTime)) {
					ip.set(ipStr);
					url.set(urlStr);
					context.write(url, ip);
				}
			} catch (ParseException e) {
				System.out.println("Parsing error for the timestamp.");
			}
		}
	}

	public static class UniqueUrlVisitsReducer extends Reducer<Text, Text, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> visits = new HashSet<String>();

			for (Text val : values) {
				visits.add(val.toString());
			}
			result.set(visits.size());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "unique url visits");
		job.setJarByClass(UniqueUrlVisitsCalculator.class);
		job.setMapperClass(UniqueUrlMapper.class);
		job.setReducerClass(UniqueUrlVisitsReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
