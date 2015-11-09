package cz.lttr.hadoop.sumbyday;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import cz.lttr.hadoop.pairs.NameDayPair;

/**
 * Sums the pagecounts for every page in every day. Filters out inappropriate
 * lines.
 */
public class SumByDayMapper extends Mapper<Object, Text, NameDayPair, IntWritable> {

	private String INPUT_FILE_NAME;
	private int MAX_LINE_LENGTH;
	private int REQUIRED_NUMBER_OF_PARTS;
	private String REQUIRED_PREFIX;
	private String FORBIDDEN_CHAR;
	private int MINIMUM_PAGECOUNT;

	/**
	 * Retrieves the name of the file that is currently processed. It contains
	 * date which is needed to complement the key.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		INPUT_FILE_NAME = ((FileSplit) context.getInputSplit()).getPath().getName();
		MAX_LINE_LENGTH = conf.getInt("MAX_LINE_LENGTH", 100);
		REQUIRED_NUMBER_OF_PARTS = conf.getInt("REQUIRED_NUMBER_OF_PARTS", 2);
		REQUIRED_PREFIX = conf.get("REQUIRED_PREFIX");
		FORBIDDEN_CHAR = conf.get("FORBIDDEN_CHAR");
		MINIMUM_PAGECOUNT = conf.getInt("MINIMUM_PAGECOUNT", 0);
	}

	/**
	 * Filters out inappropriate lines, parse them and emits key - a composite
	 * of page name and date and value - a number of page views. Thanks to the
	 * composite key, the key-value pairs will be grouped by both page name and
	 * date.
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if (line.length() > MAX_LINE_LENGTH) {
			return;
		}
		String[] parts = line.split(" ");
		if (filterLine(parts)) {
			String name = parts[1];

			String date = INPUT_FILE_NAME.split("-")[1];
			String day = date.substring(6); // last 2 from 8 characters expected (201507'01')

			int count = Integer.parseInt(parts[2]);

			context.write(new NameDayPair(name, day), new IntWritable(count));
		}
	}

	/**
	 * Method filters out lines, which are not interesting. <br>
	 * Interesting lines:
	 * <ul>
	 * <li>has at least REQUIRED_NUMBER_OF_PARTS parts</li>
	 * <li>starts with REQUIRED_PREFIX</li>
	 * <li>2nd part does not contain FORBIDDEN_CHAR</li>
	 * <li>3rd part is greater or equal to MINIMUM_PAGECOUNT</li>
	 * </ul>
	 * 
	 * @param parts
	 *            array of line parts
	 * @return true when all conditions are met
	 */
	private boolean filterLine(String[] parts) {
		if (parts.length <= REQUIRED_NUMBER_OF_PARTS) {
			return false;
		}
		if (!parts[0].equals(REQUIRED_PREFIX)) {
			return false;
		}
		if (parts[1].indexOf(FORBIDDEN_CHAR) > -1) {
			return false;
		}
		if (Integer.parseInt(parts[2]) < MINIMUM_PAGECOUNT) {
			return false;
		}
		return true;
	}
}
