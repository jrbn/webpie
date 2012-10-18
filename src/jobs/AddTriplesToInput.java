package jobs;

import mappers.io.CleanTriplesDuplicatesMapper;

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
import reducers.io.CleanDuplicatesReducer;
import writers.FilesTriplesWriter;
import data.Triple;
import data.TripleSource;

public class AddTriplesToInput extends Configured implements Tool {
	
	private static Logger log = LoggerFactory.getLogger(AddTriplesToInput.class);
	private int numReduceTasks = 1;
	private int step = -1;
	
	public void parseArgs(String[] args) {		
		for(int i=0;i<args.length; ++i) {

			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}
			
			if (args[i].equalsIgnoreCase("--step")) {
				step = Integer.valueOf(args[++i]);
			}
		}
	}	

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.out.println("Usage: AddTriplesToInput [dir] [pool] [options]");
			System.exit(0);
		}
		
		//TODO: Merge dictionary
		//TODO: assign step
		//TODO: remove duplicates
		//TODO: handle synonyms
		
		
		long time = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new AddTriplesToInput(), args);
		log.info("Execution time: " + (System.currentTimeMillis() - time));
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);
		Job job = new Job();
		job.setJobName("Cleaning duplicates");
		job.setJarByClass(AddTriplesToInput.class);
		
		job.setInputFormatClass(FilesTriplesReader.class);
		FilesTriplesReader.addInputPath(job, new Path(args[0]));
		job.getConfiguration().setInt("reasoner.filterStep", step);
		job.setMapperClass(CleanTriplesDuplicatesMapper.class);
		job.setMapOutputKeyClass(Triple.class);
		job.setMapOutputValueClass(TripleSource.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setReducerClass(CleanDuplicatesReducer.class);
		job.setOutputKeyClass(TripleSource.class);
		job.setOutputValueClass(Triple.class);
		job.setOutputFormatClass(FilesTriplesWriter.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		return 0;
	}
}
