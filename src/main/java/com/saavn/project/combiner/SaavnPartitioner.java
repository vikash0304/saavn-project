package com.saavn.project.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.saavn.project.KeyWritable;
import com.saavn.project.mapper.SaavnMapper;

/**
 * Partition data based on Date part so that input to reducer will be date which we required. 
 * @author vikash
 *
 */
public class SaavnPartitioner extends Partitioner<KeyWritable, IntWritable> {

	private static final Logger log = LoggerFactory.getLogger(SaavnPartitioner.class);
	
	public int getPartition(KeyWritable key, IntWritable value, int numReduceTasks) {
		int date = key.getDatePart();
		log.info("Date Part : {}",date);
		return date - SaavnMapper.START_DATE;
	}
}