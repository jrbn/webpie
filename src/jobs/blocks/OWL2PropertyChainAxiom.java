package jobs.blocks;

import java.io.IOException;

import mappers.owl2.OWL2PropChainCopyMapper;
import mappers.owl2.OWL2PropChainExtractMapper;
import mappers.owl2.OWL2PropChainTransMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import reducers.owl2.OWL2PropChainCopyReducer;
import reducers.owl2.OWL2PropChainTransReducer;
import data.Triple;

public class OWL2PropertyChainAxiom extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		FileSystem fs = FileSystem.get(conf);
		Path poolPath = new Path(pool.toString() + "/_property_axioms");
		Path currentPath = new Path(poolPath, "dir-current");
		if (fs.exists(poolPath)) {
			fs.delete(poolPath, true);
		}

		// Read the triples and extract the ones which might produce something
		Job job = getNewJob(
				"OWL2 reasoner: extract triples for property axiom",
				pool.toString(), "FILTER_ONLY_HIDDEN");
		job.getConfiguration().setInt("reasoner.currentExecution",
				getPreviousExecution());
		job.setMapperClass(OWL2PropChainExtractMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, currentPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		job.waitForCompletion(true);

		int step = 0;
		Path newPath = new Path(poolPath, "dir-new");
		boolean shouldContinue = true;
		while (shouldContinue) {
			job = new Job();
			job.setJarByClass(OWL2PropertyChainAxiom.class);
			job.setJobName("OWL2 reasoner: calculate transitivity on triples. Step "
					+ ++step);
			job.getConfiguration().setInt("step", step);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPathFilter(job,
					OutputLogFilter.class);
			SequenceFileInputFormat.addInputPath(job, currentPath);
			job.setNumReduceTasks(numReduceTasks);

			job.setMapperClass(OWL2PropChainTransMapper.class);
			job.setMapOutputKeyClass(BytesWritable.class);
			job.setMapOutputValueClass(BytesWritable.class);
			job.setReducerClass(OWL2PropChainTransReducer.class);
			job.setOutputKeyClass(BytesWritable.class);
			job.setOutputValueClass(BytesWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(job, newPath);
			SequenceFileOutputFormat.setCompressOutput(job, true);

			job.waitForCompletion(true);

			long counter = job.getCounters().findCounter("reasoner", "merge")
					.getValue();
			shouldContinue = counter > 0;
			fs.delete(currentPath, true);
			fs.rename(newPath, currentPath);
		}

		job = new Job();
		job.setJarByClass(OWL2PropertyChainAxiom.class);
		job.setJobName("OWL2 reasoner: convert result to triples");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPathFilter(job, OutputLogFilter.class);
		SequenceFileInputFormat.addInputPath(job, currentPath);
		job.getConfiguration().setInt("reasoner.derivationStep", executionStep);
		job.setNumReduceTasks(numReduceTasks);
		job.setMapperClass(OWL2PropChainCopyMapper.class);
		job.setMapOutputKeyClass(Triple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(OWL2PropChainCopyReducer.class);
		configureOutputJob(job, pool.toString() + OWL_OUTPUT_DIR
				+ "/dir-owl2-propaxiom-" + executionStep);
		job.waitForCompletion(true);
		setFilteredDerivation(job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"REDUCE_OUTPUT_RECORDS").getValue());

		fs.delete(poolPath, true);
	}
}
