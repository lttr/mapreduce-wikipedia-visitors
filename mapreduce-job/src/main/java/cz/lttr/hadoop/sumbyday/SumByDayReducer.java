package cz.lttr.hadoop.sumbyday;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cz.lttr.hadoop.pairs.NameDayPair;

/**
 * Creates a total sum from input values. Key is split up into two parts in
 * place of a special character.
 */
public class SumByDayReducer extends Reducer<NameDayPair, IntWritable, NameDayPair, IntWritable> {

	private IntWritable outputValue = new IntWritable();

	public void reduce(NameDayPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		outputValue.set(sum);

		context.write(key, outputValue);
	}
}
