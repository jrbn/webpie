package jobs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.NTriplesReader;

public class CountUniqueTriples {

	private static int numReducers = 1;

	public static void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {

			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReducers = Integer.valueOf(args[++i]);
			}
		}
	}

	public static class CountUniqueTuplesMapper extends
			Mapper<Text, Text, Text, NullWritable> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}

	}

	public static class CountUniqueTuplesReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;

			for (@SuppressWarnings("unused") NullWritable value : values) {
				count++;
			}

			if (count < 2) {
				context.write(key, NullWritable.get());
			}

		}

	}

	protected static Logger log = LoggerFactory
			.getLogger(CountUniqueTriples.class);

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		parseArgs(args);
		Job job = new Job();
		job.setJobName("Count unique tuples");
		job.setJarByClass(CountUniqueTriples.class);
		job.setInputFormatClass(NTriplesReader.class);
		FileInputFormat.setInputPathFilter(job, OutputLogFilter.class);
		SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(CountUniqueTuplesMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(numReducers);
		job.setReducerClass(CountUniqueTuplesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
	}

}
