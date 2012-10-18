package jobs.blocks;

import java.io.IOException;

import mappers.owl.OWLNotRecursiveMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import reducers.owl.OWLNotRecursiveReducer;

public class OWLNotRecursiveBlock extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = getNewJob("OWL reasoner: rules 1,2,3 and 8, step "
				+ executionStep, pool.toString(), "FILTER_ONLY_HIDDEN");

		job.setMapperClass(OWLNotRecursiveMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(OWLNotRecursiveReducer.class);

		String tmpDir = pool.toString() + OWL_PROP_INHERITANCE_TMP + "-"
				+ executionStep;
		configureOutputJob(job, tmpDir);
		job.waitForCompletion(true);

		// Count the derivation
		Counter derivedTriples = job.getCounters().findCounter(
				"org.apache.hadoop.mapred.Task$Counter",
				"REDUCE_OUTPUT_RECORDS");
		setNotFilteredDerivation(derivedTriples.getValue());
		if (getNotFilteredDerivation() > 0)
			setHasDerived(true);

		// Check the strategy: if always or large enough should launch the
		// cleaning up job
		long inputSize = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();
		long outputSize = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"REDUCE_OUTPUT_RECORDS").getValue();
		int ratio = (int) ((double) outputSize / inputSize * 100);
		if (outputSize > 0
				&& (getStrategy() == STRATEGY_CLEAN_DUPL_ALWAYS || (getStrategy() == STRATEGY_CLEAN_DUPL_LARGE_DERIVATION && ratio >= getDerivationRatio()))) {
			String outputDir = pool.toString() + OWL_OUTPUT_DIR
					+ "/dir-infer-properties-" + executionStep;
			setFilteredDerivation(deleteDuplicatedTriples(pool.toString(),
					tmpDir, "FILTER_ONLY_HIDDEN", outputDir,
					getFilterFromStep(), true, false, true));

			// Remove the not filtered directories
			FileSystem.get(job.getConfiguration()).delete(
					new Path(pool.toString() + RDFS_NOT_FILTERED_DIR), true);
			FileSystem.get(job.getConfiguration()).delete(
					new Path(pool.toString() + OWL_NOT_FILTERED_DIR), true);

			setFilterFromStep(executionStep);
			if (getFilteredDerivation() > 0)
				setHasDerived(true);
			else
				setHasDerived(false);
			tmpDir = outputDir;
		}
	}
}
