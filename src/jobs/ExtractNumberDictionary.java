package jobs;

import mappers.io.ExtractNumberDictionaryMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractNumberDictionary extends Configured implements Tool {

	protected static Logger log = LoggerFactory
			.getLogger(ExtractNumberDictionary.class);

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Usage: ExtractNumberDictionary [dict dir] [output dir] [URI]");
			System.exit(0);
		}

		int res = ToolRunner.run(new Configuration(),
				new ExtractNumberDictionary(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job();
		job.setJobName("Extract number from dictionary");
		job.setJarByClass(ExtractNumberDictionary.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputPathFilter(job, OutputLogFilter.class);
		SequenceFileInputFormat.addInputPath(job, new Path(args[0], "0"));
		SequenceFileInputFormat.addInputPath(job, new Path(args[0], "1"));
		job.setMapperClass(ExtractNumberDictionaryMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.getConfiguration().set("uri", args[2]);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);

		// Open file and return the IDs
		/*
		 * FileSystem fs = FileSystem.get(new Configuration()); FileStatus[]
		 * children = fs.listStatus(new Path(args[1])); for(FileStatus child :
		 * children) { if (!child.isDir()) { FSDataInputStream fis =
		 * fs.open(child.getPath()); while (true) {
		 * 
		 * } } }
		 */

		return 0;
	}
}
