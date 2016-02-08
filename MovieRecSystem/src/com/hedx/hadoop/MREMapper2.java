package com.hedx.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MREMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] items = line.split("::");
		String[] ratings = items[1].split(" ");

		// String[] ratings = line.substring(line.indexOf("("),
		// line.indexOf(")")).split(" ");

		String opKey = "";
		String opValue = "";
		String[] firstElement;
		String[] secondElement;

		for (int i = 0; i < ratings.length; i++) {
			for (int j = i + 1; j < ratings.length; j++) {
				firstElement = ratings[i].split(",");
				secondElement = ratings[j].split(",");
				if (firstElement.length == 2 && secondElement.length == 2) {
					opKey = firstElement[0] + "," + secondElement[0];
					opValue = firstElement[1] + "," + secondElement[1];
					context.write(new Text(opKey.toString()), new Text(opValue.toString()));
				}
			}
		}

	}

}
