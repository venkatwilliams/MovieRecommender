package com.hedx.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MREReducer2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String[] pairs;
		Double correlation = 0.0;
		Integer n = 0;

		Double sumX = 0.0;
		Double sumY = 0.0;
		Double sumXY = 0.0;
		Double sumXX = 0.0;
		Double sumYY = 0.0;
		Double itemX = 0.0;
		Double itemY = 0.0;

		for (Text value : values) {
			pairs = value.toString().split(",");
			System.out.println("pair[0]" + pairs.length);
			if (pairs.length == 2) {
				itemX = Double.parseDouble(pairs[0]);
				itemY = Double.parseDouble(pairs[1]);
				sumX += itemX;
				sumY += itemY;
				sumXY += itemX * itemY;
				// sumYY += Math.pow(itemY, 2);
				// sumXX += Math.pow(itemX, 2);
				sumYY += itemY * itemY;
				sumXX += itemX * itemX;
				n++;
			}
		}
		//considering movies which are rated by more than 200 users.
		if (n >= 100) {
			correlation = (sumXY) / (Math.sqrt(sumXX) * Math.sqrt(sumYY));
			context.write(key, new Text(correlation.toString()));
		}
	}

}
