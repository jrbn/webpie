package jobs;

import mappers.io.SampleNTriplesMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleNTriples extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(SampleNTriples.class);
	int sample = 100;

	public void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {

			if (args[i].equalsIgnoreCase("--sample")) {
				sample = Integer.valueOf(args[++i]);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Usage: SampleNTriples [input dir] [output dir] [--sample 1..100]");
			System.exit(0);
		}

		long time = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new SampleNTriples(),
				args);
		log.info("Execution time: " + (System.currentTimeMillis() - time));
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);

		Job job = new Job();
		job.setJarByClass(SampleNTriples.class);
		job.setJobName("Sample triples");
		job.getConfiguration().setInt("sample", sample);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(SampleNTriplesMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.waitForCompletion(true);

		return 0;
	}
}
