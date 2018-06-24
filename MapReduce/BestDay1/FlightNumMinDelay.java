package progetto;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class FlightNumMinDelay extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(FlightNumMinDelay.class);

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new FlightNumMinDelay(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "flightmindelay");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is
		// used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(4);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text lineText, Context context) throws IOException, InterruptedException {
			String file = lineText.toString();
			String lines[] = file.split("\n");

			String words[];
			for (String l : lines) {
				if (!l.contains(","))
					continue;
				words = l.split(",");

				if (!words[14].equals(""))
					context.write(new Text("FlightNum " + words[9]), new Text(words[3] + "," + words[14]));
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double min = 9999;
			int dayMin = 0;
			double average = 0;
			HashMap<String, String> m = new HashMap<String, String>();
			Integer[] ct = new Integer[7];
			String dayDelay[];
			int c;
			int d;

			for (int i = 0; i < 7; i++)
				ct[i] = 0;

			for (Text t : values) {
				dayDelay = t.toString().split(",");
				if (dayDelay[1].equals("NA"))
					continue;
				if (m.containsKey(dayDelay[0])) {
					c = Integer.parseInt((String) m.get(dayDelay[0]));
					d = c + Integer.parseInt(dayDelay[1]);
					m.put(dayDelay[0], "" + d);
				} else
					m.put(dayDelay[0], dayDelay[1]);
				ct[Integer.parseInt(dayDelay[0]) - 1]++;
			}

			double totDelay = 0;
			double count = 0;

			for (Object k : m.keySet().toArray()) {
				totDelay = Integer.parseInt((String) m.get(k));
				count = ct[Integer.parseInt((String) k) - 1];
				average = totDelay / count;
				if (average < min) {
					min = average;
					dayMin = Integer.parseInt((String) k);
				}
			}
			context.write(key, new Text("Day: " + dayMin + ", ArrDelay: " + min + "minutes."));
		}
	}
}