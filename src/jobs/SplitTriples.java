package jobs;

import mappers.io.SplitTriplesMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.Triple;
import data.TripleSource;

import readers.FilesTriplesReader;
import writers.SplitOutput;

public class SplitTriples extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(SplitTriples.class);
	int split = 100;

	public void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {

			if (args[i].equalsIgnoreCase("--splits")) {
				split = Integer.valueOf(args[++i]);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Usage: SplitTriples [input dir] [output dir] [--splits 1..n]");
			System.exit(0);
		}

		long time = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new SplitTriples(), args);
		log.info("Execution time: " + (System.currentTimeMillis() - time));
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);

		Job job = new Job();
		job.setJarByClass(SplitTriples.class);
		job.setJobName("Split triples");
		job.getConfiguration().setInt("split", split);
		job.setInputFormatClass(FilesTriplesReader.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(SplitTriplesMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(TripleSource.class);
		job.setOutputValueClass(Triple.class);
		job.setOutputFormatClass(SplitOutput.class);

		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.waitForCompletion(true);

		return 0;
	}
}
