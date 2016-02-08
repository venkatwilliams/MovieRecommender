package com.hedx.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MREMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		//String[] values = line.split("::");
		String[] values = line.split("\t");		
		String opkey = values[0];
		if(values.length >= 4){
			String opvalue = values[1] + "," + values[2];			
			context.write(new Text(opkey), new Text(opvalue));
		}		
	}

}
