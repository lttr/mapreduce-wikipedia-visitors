package cz.lttr.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Creates a total sum from input values. Key is split up into two parts in
 * place of a special character.
 */
public class VisitorsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		result.set(sum);

		String splittedKey = key.toString();
		key.set(splittedKey.replace('|', '\t'));

		context.write(key, result);
	}
}
