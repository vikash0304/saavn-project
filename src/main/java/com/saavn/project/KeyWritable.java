package com.saavn.project;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
/**
 * 
 * @author vikash
 *
 */
public class KeyWritable implements WritableComparable<KeyWritable> {

	private IntWritable date;
	private Text songId;

	public KeyWritable(IntWritable date, Text songId) {
		this.date = date;
		this.songId = songId;
	}

	public int getDatePart() {
		return this.date.get();
	}

	public String getSongId() {
		return this.songId.toString();
	}

	public KeyWritable() {
		this.date = new IntWritable(1);
		this.songId = new Text("");
	}

	public void readFields(DataInput arg0) throws IOException {
		this.date.readFields(arg0);
		this.songId.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		this.date.write(arg0);
		this.songId.write(arg0);
	}

	@Override
	public String toString() {
		return this.date + ":" + this.songId;
	}

	@Override
	public int hashCode() {
		return date.hashCode() + songId.hashCode() * 163;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof KeyWritable) {
			KeyWritable tp = (KeyWritable) o;
			return date.equals(tp.date) && songId.equals(tp.songId);
		}
		return false;
	}

	public int compareTo(KeyWritable arg0) {
		int cmp = date.compareTo(arg0.date);
		if (cmp != 0) {
			return cmp;
		}

		return songId.compareTo(arg0.songId);
	}
}
