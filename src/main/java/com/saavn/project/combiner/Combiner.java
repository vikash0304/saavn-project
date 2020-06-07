package com.saavn.project.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.saavn.project.KeyWritable;

/**
 * 
 * @author vikash
 *
 */
public class Combiner extends Reducer<KeyWritable, IntWritable, KeyWritable, IntWritable>{
	
	private static final Logger log = LoggerFactory.getLogger(Combiner.class);
	
	@Override
	protected void reduce(KeyWritable keyWritable, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int count = 0;
        for(IntWritable val : values) {
        	count = count + val.get();
        }
        log.info("Total count : {}",count);

        context.write(keyWritable, new IntWritable(count));
	}
}
