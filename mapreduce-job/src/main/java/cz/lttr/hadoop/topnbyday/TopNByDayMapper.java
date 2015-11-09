package cz.lttr.hadoop.topnbyday;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cz.lttr.hadoop.pairs.NameDayPair;

public class TopNByDayMapper extends Mapper<Object, Text, NameDayPair, IntWritable> {

	/** List of top N page names with highest page views. */
	private List<String> topNNames;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path path = new Path(conf.get("TOTAL_SUM_INPUT_FILES"));
		try {
			topNNames = readLines(path, conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		ListIterator<String> iterator = topNNames.listIterator();
		String line = value.toString();
		String[] parts = line.split("\t");
		String name = parts[0];

		while (iterator.hasNext()) {
			if (name.equals(iterator.next())) {
				String day = parts[1];
				int count = Integer.parseInt(parts[2]);

				NameDayPair nameDayPair = new NameDayPair(name, day);
				context.write(nameDayPair, new IntWritable(count));

				iterator.remove();
				break;
			}
		}
	}

	private List<String> readLines(Path location, Configuration conf) throws Exception {
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		FileStatus[] items = fileSystem.listStatus(location);
		if (items == null)
			return new ArrayList<String>();
		List<String> results = new ArrayList<String>();
		// for each file in location
		for (FileStatus item : items) {
			if (item.getPath().getName().startsWith("_")) { // ignoring files like _SUCCESS
				continue;
			}
			InputStream stream = null;

			stream = fileSystem.open(item.getPath());
			StringWriter writer = new StringWriter();
			IOUtils.copy(stream, writer, "UTF-8");
			String raw = writer.toString();

			for (String line : raw.split("\n")) {
				String firstField = line.split("\t")[0]; // interested in the first field only
				results.add(firstField);
			}
		}
		return results;
	}
}
