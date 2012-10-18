package jobs.blocks;

import java.io.IOException;

import mappers.owl.OWLAllSomeValuesMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;

import reducers.owl.OWLAllSomeValuesReducer;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class OWLSomeAllValuesBlock extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = getNewJob("OWL reasoner: some and all values rule. step "
				+ executionStep, pool.toString(), "FILTER_ONLY_HIDDEN");

		job.setMapperClass(OWLAllSomeValuesMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(ProtobufWritable.class);
		job.setReducerClass(OWLAllSomeValuesReducer.class);

		String tmpDir = pool.toString() + OWL_ALL_VALUE_TMP + "-"
				+ executionStep;
		configureOutputJob(job, tmpDir);

		job.waitForCompletion(true);
		long derivation = job.getCounters().findCounter(
				"org.apache.hadoop.mapred.Task$Counter",
				"REDUCE_OUTPUT_RECORDS").getValue();

		setNotFilteredDerivation(derivation);
		if (derivation > 0)
			setHasDerived(true);

		if (getNotFilteredDerivation() > 0) {

			long inputSize = job.getCounters().findCounter(
					"org.apache.hadoop.mapred.Task$Counter",
					"MAP_INPUT_RECORDS").getValue();
			int ratio = (int) ((double) derivation / inputSize * 100);
			if (getStrategy() == STRATEGY_CLEAN_DUPL_ALWAYS
					|| (getStrategy() == STRATEGY_CLEAN_DUPL_LARGE_DERIVATION && ratio > getDerivationRatio())) {
				String outputDir = pool.toString() + OWL_OUTPUT_DIR
						+ "/dir-all-some-statements-" + executionStep;
				long currentDerivation = deleteDuplicatedTriples(pool
						.toString(), tmpDir, "FILTER_ONLY_HIDDEN", outputDir,
						getFilterFromStep(), true, false, true);

				setFilterFromStep(executionStep);
				setFilteredDerivation(currentDerivation);

				if (currentDerivation > 0)
					setHasDerived(true);
				else
					setHasDerived(false);

				// Remove the not filtered directories
				FileSystem.get(job.getConfiguration())
						.delete(
								new Path(pool.toString()
										+ RDFS_NOT_FILTERED_DIR), true);
				FileSystem.get(job.getConfiguration()).delete(
						new Path(pool.toString() + OWL_NOT_FILTERED_DIR), true);

			}
			return;
		} else {
			FileSystem.get(job.getConfiguration()).delete(new Path(tmpDir),
					true);
		}
	}
}
