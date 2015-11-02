package cz.lttr.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class VisitorsPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		String keyString = key.toString();
		int keyInt = Integer.parseInt(keyString.substring(keyString.length() - 2));
		return (keyInt - 1) % numPartitions;
	}

}
