package com.saavn.project.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.saavn.project.KeyWritable;

/**
 * SaavnMapper class take input to mapper class for provided and it was a (,) separated value
 * @author vikash
 *
 */
public class SaavnMapper extends Mapper<LongWritable, Text, KeyWritable, IntWritable > {
	
	public static final int START_DATE = 24;
	private static final int END_DATE = 31;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		try {			
			String[] valueItems = value.toString().split(",");			
			String songId = valueItems[0]; 
		
			int datePart = Integer.parseInt(valueItems[4].toString().split("-")[2]);
			int hour = Integer.parseInt(valueItems[3].toString());
			if(!songId.isEmpty() && !songId.equals("(null)") && datePart >= START_DATE && datePart <=END_DATE)
			{				
				KeyWritable outputKey = new KeyWritable(new IntWritable(datePart), new Text(songId));
				IntWritable weight = new IntWritable(hour * 1);
				context.write(outputKey, weight);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
