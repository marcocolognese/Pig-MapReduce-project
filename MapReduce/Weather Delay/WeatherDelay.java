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

public class WeatherDelay extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(WeatherDelay.class);

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new WeatherDelay(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "weatherdelay");
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

				if (!words[14].equals("") && !words[25].equals(""))
					context.write(new Text("Year " + words[0]), new Text(words[14] + "," + words[25]));
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double avgArrDelay = 0;
			double avgWeatherDelay = 0;
			double ct = 0;
			String s[];

			for (Text t : values) {
				s = t.toString().split(",");
				if (s[0].equals("NA") || s[1].equals("NA"))
					continue;
				else {
					avgArrDelay += Integer.parseInt(s[0]);
					avgWeatherDelay += Integer.parseInt(s[1]);
					ct++;
				}
			}

			avgArrDelay = avgArrDelay / ct;
			avgWeatherDelay = avgWeatherDelay / ct;

			context.write(key, new Text(
					"ArrDelay: " + avgArrDelay + " minutes.\n\t\t" + "WeatherDelay: " + avgWeatherDelay + " minutes."));
		}
	}
}