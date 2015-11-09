package cz.lttr.hadoop.totaltopn;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cz.lttr.hadoop.pairs.TextIntPair;

/**
 * Creates a total sum from input values. Key is left unchanged.
 */
public class TotalTopNReducer extends Reducer<Text, IntWritable, TextIntPair, IntWritable> {

	private Map<String, Integer> hashMap = new HashMap<>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		hashMap.put(key.toString(), sum);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int N = conf.getInt("N", 10);

		SortedSet<Map.Entry<String, Integer>> sortedSet = new TreeSet<Map.Entry<String, Integer>>(
				new Comparator<Map.Entry<String, Integer>>() {
					@Override
					public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
						int result = -1 * e1.getValue().compareTo(e2.getValue());
						return result != 0 ? result : e1.getKey().compareTo(e2.getKey()); // preserve equal values
					}
				});

		sortedSet.addAll(hashMap.entrySet());

		Iterator<Entry<String, Integer>> it = sortedSet.iterator();
		int orderNumber = 1;
		while (it.hasNext() && orderNumber <= N) {
			Entry<String, Integer> entry = it.next();
			context.write(new TextIntPair(entry.getKey(), orderNumber), new IntWritable(entry.getValue()));
			orderNumber++;
		}
	}
}
