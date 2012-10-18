package jobs;

import mappers.io.CheckConsistencyCompressionMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.MultiFilesReader;
import readers.NTriplesReader;
import reducers.io.CheckConsistencyCompressionReducer;

public class CheckConsistencyDictionary extends Configured implements Tool {

	private static Logger log = LoggerFactory
			.getLogger(CheckConsistencyDictionary.class);
	private int numReduceTasks = 1;
	private int numMapTasks = -1;

	public void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equalsIgnoreCase("--maptasks")) {
				numMapTasks = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Usage: CheckConsistencyDictionary [pool] [output]");
			System.exit(0);
		}

		int res = ToolRunner.run(new Configuration(),
				new CheckConsistencyDictionary(), args);
		log.info("The compression is consistent!");
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);

		Job job = new Job();
		job.setJobName("Checking consistency");

		MultiFilesReader.setSplitable(job.getConfiguration(), false);
		job.setInputFormatClass(NTriplesReader.class);
		FileInputFormat.setInputPathFilter(job, OutputLogFilter.class);
		job.getConfiguration().setInt("maptasks", numMapTasks);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(CheckConsistencyCompressionMapper.class);
		job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setReducerClass(CheckConsistencyCompressionReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);

		return 0;
	}
}
