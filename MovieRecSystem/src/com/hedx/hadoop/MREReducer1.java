package com.hedx.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MREReducer1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Integer itemCount = 0;
		Integer itemSum = 0;
		String opValue = "";
		String ratings = "";

		Iterator<Text> rating = values.iterator();

		//ratings = "(";
		while (rating.hasNext()) {

			Text ratingValue = (Text) rating.next();
			String value = ratingValue.toString();
			String[] ratingPair = value.split(",");

			if (ratingPair.length == 2) {
				// itemId = Integer.parseInt(ratingPair[0]);
				itemSum += Integer.parseInt(ratingPair[1]);
				itemCount++;

			} else {
				System.exit(-1);
			}
			
			ratings += " "+  ratingValue.toString() +" ";
		}
		//ratings += ")";

		opValue += "" + itemCount;
		opValue += "," + itemSum;
		opValue += "::" + ratings;

		context.write(key, new Text(opValue));

	}

}
