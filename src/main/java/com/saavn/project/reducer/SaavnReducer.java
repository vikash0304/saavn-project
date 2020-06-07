package com.saavn.project.reducer;

import java.io.IOException;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.saavn.project.KeyWritable;

/**
 *  This class will take value of specific key-value.
 *  Using a HashMap function to store intermediate value in Reducer format
 * 
 * @author vikash
 */
@SuppressWarnings("rawtypes")
public class SaavnReducer extends Reducer<KeyWritable, IntWritable, Text, IntWritable>{	
	private HashMap<String, Integer> intermediateOutput = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(KeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {		
		int count = 0;
        for(IntWritable val : values) {
        	count = count + val.get();
        }

        intermediateOutput.put(key.getSongId(), count);
	}
	
	@SuppressWarnings("unchecked")
	protected void cleanup(Context context) throws IOException, InterruptedException {
		List listOfSongsWithCount = new LinkedList(intermediateOutput.entrySet());

		Collections.sort(listOfSongsWithCount, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue())
	                  .compareTo(((Map.Entry) (o1)).getValue());
			}
		});

		HashMap sortedHashMap = new LinkedHashMap();
		Iterator iterator = listOfSongsWithCount.iterator();
		while (iterator.hasNext()) {
	         Map.Entry entry = (Map.Entry) iterator.next();
	         sortedHashMap.put(entry.getKey(), entry.getValue());
		}
		int counter = 0;
		for (Object key: sortedHashMap.keySet())
		{
			counter = counter + 1;
			context.write(new Text(key.toString()), new IntWritable(Integer.parseInt(sortedHashMap.get(key).toString())));
			if (counter == 100) {
				break;
			}
		}
	}
}
