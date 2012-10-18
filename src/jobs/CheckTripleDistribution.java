package jobs;

import mappers.io.CheckTripleDistributionMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import reducers.io.CheckTripleDistributionReducer;

public class CheckTripleDistribution extends Configured implements Tool {
	
	private static Logger log = LoggerFactory.getLogger(CheckTripleDistribution.class);
	private int numMapTasks = 1;
	private int numReduceTasks = 1;
	private int partitions = 1;
	
	public void parseArgs(String[] args) {		
		for(int i=0;i<args.length; ++i) {
			
			if (args[i].equalsIgnoreCase("--maptasks")) {
				numMapTasks = Integer.valueOf(args[++i]);
			}
			
			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}
			
			if (args[i].equalsIgnoreCase("--partitions")) {
				partitions = Integer.valueOf(args[++i]);
			}
		}
	}	

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.out.println("Usage: CheckTripleDistribution [input dir] [output dir] [options]");
			System.exit(0);
		}
		
		long time = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new CheckTripleDistribution(), args);
		log.info("Execution time: " + (System.currentTimeMillis() - time));
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);
		Job job = new Job();
		job.setJobName("Check distribution hashcodes");
		job.setJarByClass(CheckTripleDistribution.class);
		FileSystem.get(new Configuration()).delete(new Path(args[1]), true);
		
		job.setInputFormatClass(FilesTriplesReader.class);
		FilesTriplesReader.addInputPath(job, new Path(args[0]));
		job.getConfiguration().setInt("maptasks", numMapTasks);
		job.getConfiguration().setInt("partitions", partitions);
		job.setMapperClass(CheckTripleDistributionMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setReducerClass(CheckTripleDistributionReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		return 0;
	}
}
