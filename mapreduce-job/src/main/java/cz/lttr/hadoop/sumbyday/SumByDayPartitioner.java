package cz.lttr.hadoop.sumbyday;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import cz.lttr.hadoop.pairs.NameDayPair;

/**
 * Partitions a set of intermediate key value pairs in format like:
 * (name+day)(count) by day.
 */
public class SumByDayPartitioner extends Partitioner<NameDayPair, IntWritable> {

	@Override
	public int getPartition(NameDayPair key, IntWritable value, int numPartitions) {
		String dayString = key.getDay();
		int dayInt = Integer.parseInt(dayString);
		return (dayInt - 1) % numPartitions;
	}

}
