package cz.lttr.hadoop.topnbyday;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import cz.lttr.hadoop.pairs.NameDayPair;

public class TopNByDayPartitioner extends Partitioner<NameDayPair, IntWritable> {

	@Override
	public int getPartition(NameDayPair key, IntWritable value, int numPartitions) {
		String nameString = key.getName();
		return (nameString.hashCode()) % numPartitions;
	}

}
