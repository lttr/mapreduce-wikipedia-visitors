package cz.lttr.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job consists of two MapReduce sub-jobs. It picks up data from input directory
 * (the first argument). The first job sums the page views grouped by page name
 * and date and saves the output to intermediate directory (the second
 * argument). The second job sums the total page views for every page and saves
 * the output to final directory (the third argument).
 */
public class WikipediaVisitorsJob {

	public static void main(String[] args) {

		Path inputPath = new Path(args[0]);
		Path sumByDayPath = new Path(args[1]);
		Path totalSumPath = new Path(args[2]);

		try {
			sumVisitorsByDay(inputPath, sumByDayPath);
		} catch (Exception e) {
			System.err.println("Exception running job sumVisitorsByDay: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

		try {
			sumTotalVisitors(sumByDayPath, totalSumPath);
		} catch (Exception e) {
			System.err.println("Exception running job sumTotalVisitors: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

	}

	private static void sumVisitorsByDay(Path inputPath, Path outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "sumVisitorsByDay");

		job.setJarByClass(WikipediaVisitorsJob.class);
		job.setMapperClass(VisitorsMapper.class);
		job.setCombinerClass(VisitorsReducer.class);
		job.setReducerClass(VisitorsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}

	private static void sumTotalVisitors(Path inputPath, Path outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "sumTotalVisitors");

		job.setJarByClass(WikipediaVisitorsJob.class);
		job.setMapperClass(SumVisitorsMapper.class);
		job.setCombinerClass(SumVisitorsReducer.class);
		job.setReducerClass(SumVisitorsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}

}
