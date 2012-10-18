package jobs;

import mappers.io.AssignStepMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import writers.FilesTriplesWriter;

import data.Triple;
import data.TripleSource;

public class AssignStepToTriples extends Configured implements Tool {
	
	private static Logger log = LoggerFactory.getLogger(AssignStepToTriples.class);
	private int step = 1;
	
	public void parseArgs(String[] args) {		
		for(int i=0;i<args.length; ++i) {
		
			if (args[i].equalsIgnoreCase("--step")) {
				step = Integer.valueOf(args[++i]);
			}
		}
	}	

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.out.println("Usage: AssignStepToTriples [input dir] [output dir]");
			System.exit(0);
		}
		
		long time = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new AssignStepToTriples(), args);
		log.info("Execution time: " + (System.currentTimeMillis() - time));
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);
		
		Job job = new Job();
		job.setJarByClass(AssignStepToTriples.class);
		job.setJobName("Assign step " + step + " to triples");
		job.getConfiguration().setLong("step", step);
		
		job.setInputFormatClass(FilesTriplesReader.class);
		FilesTriplesReader.addInputPath(job, new Path(args[0]));
		job.setMapperClass(AssignStepMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(TripleSource.class);
		job.setOutputValueClass(Triple.class);
		job.setOutputFormatClass(FilesTriplesWriter.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		return 0;
	}
}
