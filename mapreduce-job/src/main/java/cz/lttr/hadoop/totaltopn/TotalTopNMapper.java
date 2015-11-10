package cz.lttr.hadoop.totaltopn;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Maps the page name and page count as key and value.
 */
public class TotalTopNMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text name = new Text();
	private IntWritable count = new IntWritable();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] parts = line.split("\t");

		name.set(parts[0]);
		count.set(Integer.parseInt(parts[2]));
		context.write(name, count);

	}
}
