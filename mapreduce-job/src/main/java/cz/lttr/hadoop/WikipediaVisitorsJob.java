package cz.lttr.hadoop;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cz.lttr.hadoop.pairs.NameDayPair;
import cz.lttr.hadoop.pairs.TextIntPair;
import cz.lttr.hadoop.sumbyday.SumByDayMapper;
import cz.lttr.hadoop.sumbyday.SumByDayPartitioner;
import cz.lttr.hadoop.sumbyday.SumByDayReducer;
import cz.lttr.hadoop.topnbyday.TopNByDayMapper;
import cz.lttr.hadoop.topnbyday.TopNByDayReducer;
import cz.lttr.hadoop.totaltopn.TotalTopNMapper;
import cz.lttr.hadoop.totaltopn.TotalTopNReducer;

/**
 * Job consists of three MapReduce sub-jobs. It picks up data from input
 * directory (the first argument). The first job sums the page views grouped by
 * page name and date and saves the output to intermediate directory. The second
 * job sums the total page views for every page and saves the top N to second
 * intermediate directory. The third job combines the intermediate data into
 * final result.
 */
public class WikipediaVisitorsJob {

	/** Number of pages with top view counts. */
	public static final int N = 20;

	/** Prefix for czech pages from Wikipedia project. */
	public static final String REQUIRED_PREFIX = "cs";

	/**
	 * Minimum number of page views can significantly lower the amount of input
	 * data. Convenient for testing.
	 */
	public static final int MINIMUM_PAGEVIEWS = 0;

	/** The required number of columns in input data. */
	public static final int REQUIRED_NUMBER_OF_PARTS = 3;

	/** I am not interested in special pages which contain colon. */
	public static final String FORBIDDEN_CHAR = ":";

	/** Too long lines are typical for other content than normal pages. */
	public static final int MAX_LINE_LENGTH = 120;

	/** Format for unique name of output folder. */
	public static final String OUTPUT_FOLDER_FORMAT = "yyyy-MM-dd-HH-mm-ss";

	/* Name of output folder for each job */
	public static final String SUM_BY_DAY_FOLDER = "sum-by-day";
	public static final String TOTAL_SUM_FOLDER = "total-top-n";
	public static final String TOP_N_BY_DAY_FOLDER = "top-n-by-day";

	// main method
	public static void main(String[] args) {

		Path inputPath = new Path(args[0]);
		String outBasePath = args[1];

		DateFormat dateFormat = new SimpleDateFormat(OUTPUT_FOLDER_FORMAT);
		Date now = new Date();
		String commonOutFolder = dateFormat.format(now);

		Path sumByDayPath = new Path(outBasePath + "/" + commonOutFolder + "/" + SUM_BY_DAY_FOLDER);
		Path totalTopNPath = new Path(outBasePath + "/" + commonOutFolder + "/" + TOTAL_SUM_FOLDER);
		Path topNByDayPath = new Path(outBasePath + "/" + commonOutFolder + "/" + TOP_N_BY_DAY_FOLDER);

		try {
			sumByDayJob(inputPath, sumByDayPath);
		} catch (Exception e) {
			System.err.println("Exception running job sumByDayJob: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

		try {
			totalTopNJob(sumByDayPath, totalTopNPath);
		} catch (Exception e) {
			System.err.println("Exception running job totalTopNJob: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

		try {
			topNByDayJob(sumByDayPath, totalTopNPath, topNByDayPath);
		} catch (Exception e) {
			System.err.println("Exception running job topNByDayJob: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

	}

	private static void sumByDayJob(Path inputPath, Path outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("REQUIRED_PREFIX", REQUIRED_PREFIX);
		conf.setInt("MINIMUM_PAGEVIEWS", MINIMUM_PAGEVIEWS);
		conf.setInt("REQUIRED_NUMBER_OF_PARTS", REQUIRED_NUMBER_OF_PARTS);
		conf.set("FORBIDDEN_CHAR", FORBIDDEN_CHAR);
		conf.setInt("MAX_LINE_LENGTH", MAX_LINE_LENGTH);

		Job job = Job.getInstance(conf, "sumByDayJob");
		job.setJarByClass(WikipediaVisitorsJob.class);

		job.setNumReduceTasks(31);

		job.setMapperClass(SumByDayMapper.class);
		job.setPartitionerClass(SumByDayPartitioner.class);
		job.setCombinerClass(SumByDayReducer.class);
		job.setReducerClass(SumByDayReducer.class);

		job.setOutputKeyClass(NameDayPair.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}

	private static void totalTopNJob(Path inputPath, Path outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.setInt("N", N);

		Job job = Job.getInstance(conf, "totalTopNJob");
		job.setJarByClass(WikipediaVisitorsJob.class);

		job.setNumReduceTasks(1);

		job.setMapperClass(TotalTopNMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(TotalTopNReducer.class);
		job.setOutputKeyClass(TextIntPair.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}

	private static void topNByDayJob(Path byDayInputPath, Path totalTopNInputPath, Path outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("TOTAL_SUM_INPUT_FILES", totalTopNInputPath.toString());

		Job job = Job.getInstance(conf, "topNByDayJob");
		job.setJarByClass(WikipediaVisitorsJob.class);

		job.setNumReduceTasks(1);

		job.setMapperClass(TopNByDayMapper.class);
		job.setReducerClass(TopNByDayReducer.class);

		job.setOutputKeyClass(NameDayPair.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, byDayInputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}
}
