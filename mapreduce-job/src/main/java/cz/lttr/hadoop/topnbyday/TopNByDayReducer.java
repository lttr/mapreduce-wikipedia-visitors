package cz.lttr.hadoop.topnbyday;

import java.io.IOException;
import java.net.URLDecoder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cz.lttr.hadoop.pairs.NameDayPair;

public class TopNByDayReducer extends Reducer<NameDayPair, IntWritable, NameDayPair, IntWritable> {

	@Override
	protected void reduce(NameDayPair nameDayPair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		for (IntWritable count : values) {
			// url-decode the page name, make sure it does not contain harmful characters
			try {
				nameDayPair.setName(URLDecoder.decode(nameDayPair.getName(), "UTF-8"));
			} catch (Exception e) {
				System.err.println("Unable to decode pageName: " + nameDayPair.getName() + " with error message: " + e.getMessage());
				e.printStackTrace();
			}
			context.write(nameDayPair, count);
		}
	}

}
