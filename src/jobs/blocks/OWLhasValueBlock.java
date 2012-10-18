package jobs.blocks;


import java.io.IOException;
import java.util.List;

import mappers.owl.OWLHasValueMapper;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import readers.MultiFilesReader;
import reducers.owl.OWLHasValueReducer;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class OWLhasValueBlock extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = getNewJob("OWL reasoner: hasValue rule. Step "
				+ executionStep, pool.toString(), "FILTER_ONLY_HIDDEN");

		List<FileStatus> files = MultiFilesReader.recursiveListStatus(job,
				"FILTER_ONLY_OWL_HAS_VALUE");
		if (files == null || files.size() == 0) {
			return; // There is no schema information
		}
		files = MultiFilesReader.recursiveListStatus(job,
				"FILTER_ONLY_OWL_ON_PROPERTY");
		if (files == null || files.size() == 0) {
			return; // There is no schema information
		}

		job.setMapperClass(OWLHasValueMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ProtobufWritable.class);
		job.setReducerClass(OWLHasValueReducer.class);
		String tmpDir = pool.toString() + OWL_NOT_FILTERED_DIR + OWL_HAS_VALUE_TMP
				+ "-" + executionStep;
		configureOutputJob(job, tmpDir);
		job.waitForCompletion(true);

		setNotFilteredDerivation(job.getCounters().findCounter(
				"org.apache.hadoop.mapred.Task$Counter",
				"REDUCE_OUTPUT_RECORDS").getValue());
		// Delete duplicated triples
		if (getNotFilteredDerivation() > 0) {
			long input = job.getCounters().findCounter(
					"org.apache.hadoop.mapred.Task$Counter",
					"MAP_INPUT_RECORDS").getValue();
			int ratio = (int) ((double) getNotFilteredDerivation() / input * 100);
			if (getStrategy() == STRATEGY_CLEAN_DUPL_ALWAYS
					|| (getStrategy() == STRATEGY_CLEAN_DUPL_LARGE_DERIVATION && ratio > getDerivationRatio())) {
				String outputDir = pool.toString() + OWL_OUTPUT_DIR
						+ "/dir-has-value-" + executionStep;
				setFilteredDerivation(deleteDuplicatedTriples(pool
						.toString(), tmpDir, "FILTER_ONLY_HIDDEN", outputDir,
						getFilterFromStep(), true, false, true));

				//Remove the not filtered directories
				FileSystem.get(job.getConfiguration()).delete(new Path(pool.toString() + RDFS_NOT_FILTERED_DIR), true);
				FileSystem.get(job.getConfiguration()).delete(new Path(pool.toString() + OWL_NOT_FILTERED_DIR), true);

				setFilterFromStep(executionStep);
				setHasDerived(getFilteredDerivation() > 0);
			} else {
				setHasDerived(true);
			}
		} else {
			FileSystem fs = FileSystem.get(job.getConfiguration());
			fs.delete(new Path(tmpDir), true);
		}
	}
}
