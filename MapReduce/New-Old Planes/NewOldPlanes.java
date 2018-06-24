package progetto;

import java.io.IOException;

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

public class NewOldPlanes extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(NewOldPlanes.class);

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new NewOldPlanes(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "newoldplanes");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is
		// used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(8);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text lineText, Context context) throws IOException, InterruptedException {
			String plane = null;
			String file = lineText.toString();
			String lines[] = file.split("\n");

			String words[];
			for (String l : lines) {
				if (!l.contains(","))
					continue;
				words = l.split(",");

				if (Integer.parseInt(words[0]) > 2004)
					plane = "New Planes";
				else
					plane = "Old Planes";

				if (!words[14].isEmpty())
					context.write(new Text(plane), new Text(words[14]));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double average = 0;
			double ct = 0;

			for (Text t : values) {
				if (t.toString().equals("NA"))
					continue;
				else {
					average += Integer.parseInt(t.toString());
					ct++;
				}
			}

			average = average / ct;
			context.write(key, new Text("ArrDelay: " + average + " minutes."));
		}
	}
}