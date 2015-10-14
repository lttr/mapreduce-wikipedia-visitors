package cz.lttr.hadoop;

import java.io.IOException;
import java.net.URLDecoder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Sums the pagecounts for every page in every day. Filters out inappropriate
 * lines.
 */
public class VisitorsMapper extends Mapper<Object, Text, Text, IntWritable> {

	/** I am interested only in czech pages from Wikipedia project */
	private static final String REQUIRED_PREFIX = "cs";
	/**
	 * Minimum number of page views can significantly lower the amount of input
	 * data. Convenient for testing.
	 */
	private static final int MINIMUM_PAGEVIEWS = 0;
	/** The required number of columns in input data */
	private static final int REQUIRED_NUMBER_OF_PARTS = 3;
	/** I am not interested in special pages which contain colon. */
	private static final char FORBIDDEN_CHAR = ':';
	/** Too long lines are typical for other content than normal pages. */
	private static final int MAX_LINE_LENGTH = 120;

	private String fileName;

	/**
	 * Retrieves the name of the file that is currently processed. It contains
	 * date which is needed to complement the key.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
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
		if (line.length() < MAX_LINE_LENGTH) {

			String[] parts = line.split(" ");
			if (filterLine(parts)) {
				String pageName = parts[1];

				// url-decode the page name, make sure it does not contain harmful characters
				try {
					pageName = URLDecoder.decode(pageName, "UTF-8");
				} catch (Exception e) {
					System.err.println("Unable to decode pageName: " + pageName + " with error message: " + e.getMessage());
					e.printStackTrace();
				} finally {
					pageName = pageName.replace(' ', '_');
					pageName = pageName.replace('|', '_');
				}

				String date = fileName.split("-")[1];
				Text outputKey = new Text(pageName + "|" + date);
				IntWritable pageCount = new IntWritable(Integer.parseInt(parts[2]));

				context.write(outputKey, pageCount);
			}
		}
	}

	/**
	 * Method filters out lines, which are not interesting. <br>
	 * Interesting lines:
	 * <ul>
	 * <li>has at least REQUIRED_NUMBER_OF_PARTS parts</li>
	 * <li>starts with REQUIRED_PREFIX</li>
	 * <li>2nd part does not contain FORBIDDEN_CHAR</li>
	 * <li>3rd part is greater or equal to MINIMUM_PAGECOUNTS</li>
	 * </ul>
	 * 
	 * @param parts
	 *            array of line parts
	 * @return true when all conditions are met
	 */
	private boolean filterLine(String[] parts) {
		if (parts.length >= REQUIRED_NUMBER_OF_PARTS) {
			if (parts[0].equals(REQUIRED_PREFIX)) {
				if (parts[1].indexOf(FORBIDDEN_CHAR) == -1) {
					if (Integer.parseInt(parts[2]) >= MINIMUM_PAGEVIEWS) {
						return true;
					}
				}
			}
		}
		return false;
	}
}
