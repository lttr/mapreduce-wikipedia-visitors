package cz.lttr.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This is basically an identity mapper, it only selects the first record as key
 * and the third as value.
 */
public class SumVisitorsMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split("\t");

		if (parts.length > 2) {
			Text pageName = new Text(parts[0]);
			IntWritable pageCount = new IntWritable(Integer.parseInt(parts[2]));
			context.write(pageName, pageCount);
		}

	}
}
