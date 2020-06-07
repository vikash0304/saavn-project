package com.saavn.project.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.saavn.project.KeyWritable;
import com.saavn.project.mapper.SaavnMapper;

/**
 * Partition data based on Date part so that input to reducer will be date which we required. 
 * @author vikash
 *
 */
public class SaavnPartitioner extends Partitioner<KeyWritable, IntWritable> {

	public int getPartition(KeyWritable key, IntWritable value, int numReduceTasks) {
		int date = key.getDatePart();
		return date - SaavnMapper.START_DATE;
	}
}