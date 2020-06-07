package com.saavn.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.saavn.project.combiner.Combiner;
/**
 * Driver class contain all configuration required setting for job
 * @author vikash
 *
 */
public class SaavnDriver extends Configured implements Tool {
	
	private static final Logger log = LoggerFactory.getLogger(SaavnDriver.class);
	
	private static final String JOB_NAME = "Saavn Top Trending songs";
	
	public static void main(String[] args) throws Exception {
		log.info("input parameters: {}",args);
		int returnStatus = ToolRunner.run(new Configuration(),
				new SaavnDriver(), args);
		System.exit(returnStatus);
	}

	public int run(String[] args) throws IOException {
		Job job = new Job(getConf());
		job.setJarByClass(SaavnDriver.class);
		log.info(JOB_NAME);
		job.setJobName(JOB_NAME);

		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(KeyWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(7);
		job.setPartitionerClass(Partitioner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		try {
			return job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			log.error("Unable to find class: {}",e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			log.error("Interrupted Exception: {}",e.getMessage());
			e.printStackTrace();
		}
		return 0;
	}
}
