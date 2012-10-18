package jobs.blocks;

import java.io.IOException;

import mappers.owl2.OWL2HasKey1Mapper;
import mappers.owl2.OWL2IdentityMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import reducers.owl2.OWL2HasKey1Reducer;
import reducers.owl2.OWL2HasKey2Reducer;

public class OWL2HasKey extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {
		Path tmpPath = new Path(pool.toString() + OWL_NOT_FILTERED_DIR
				+ "/dir-owl2-has-key1-" + executionStep);

		Job job = getNewJob("OWL2 reasoner: hasKey part 1", pool.toString(),
				"FILTER_ONLY_HIDDEN");

		job.setMapperClass(OWL2HasKey1Mapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(OWL2HasKey1Reducer.class);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputPath(job, tmpPath);

		job.waitForCompletion(true);
		long inputSize = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

		// Second job: read the outcome and return the sameAs relations
		job = new Job();
		job.setJobName("OWL2 reasoner: hasKey part 2");
		job.setJarByClass(OWL2HasKey.class);
		job.getConfiguration().setInt("reasoner.step", executionStep);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, tmpPath);
		job.setMapperClass(OWL2IdentityMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setReducerClass(OWL2HasKey2Reducer.class);
		String rulesOutput = pool.toString() + OWL_NOT_FILTERED_DIR
				+ "/dir-owl2-has-key2-" + executionStep;
		configureOutputJob(job, rulesOutput);

		job.waitForCompletion(true);

		long outputSize = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"REDUCE_OUTPUT_RECORDS").getValue();
		setNotFilteredDerivation(outputSize);
		int ratio = (int) ((double) outputSize / inputSize * 100);
		String outputDir = pool.toString() + OWL_SYNONYMS_TABLE + "/dir-step-"
				+ executionStep;
		if (outputSize > 0
				&& (getStrategy() == STRATEGY_CLEAN_DUPL_ALWAYS || (getStrategy() == STRATEGY_CLEAN_DUPL_LARGE_DERIVATION && ratio >= getDerivationRatio()))) {

			long derivation = deleteDuplicatedTriples(pool.toString(),
					rulesOutput, "FILTER_ONLY_SAME_AS", outputDir,
					executionStep - 1, true, false, true);
			setFilteredDerivation(derivation);

			if (getFilteredDerivation() > 0)
				setHasDerived(true);
		} else {
			setHasDerived(outputSize > 0);

			FileSystem.get(job.getConfiguration()).rename(
					new Path(rulesOutput), new Path(outputDir));
		}

		FileSystem.get(job.getConfiguration()).delete(tmpPath, true);
	}
}
