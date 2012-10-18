package jobs.blocks;

import java.io.IOException;

import mappers.rdfs.RDFSSubPropDomRangeMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import reducers.rdfs.RDFSSubpropDomRangeReducer;

public class RDFSDomainRangeBlock extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = getNewJob("RDFS domain range reasoning. Step "
				+ executionStep, pool.toString(), "FILTER_ONLY_HIDDEN");

		job.setMapperClass(RDFSSubPropDomRangeMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(RDFSSubpropDomRangeReducer.class);
		String outputDir = pool.toString() + RDFS_NOT_FILTERED_DIR
				+ "/dir-subprop-domain-range-" + executionStep;
		configureOutputJob(job, outputDir);

		job.waitForCompletion(true);

		setNotFilteredDerivation(job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"REDUCE_OUTPUT_RECORDS").getValue());
		if (getNotFilteredDerivation() > 0) {
			long inputSize = job
					.getCounters()
					.findCounter("org.apache.hadoop.mapred.Task$Counter",
							"MAP_INPUT_RECORDS").getValue();
			int ratio = (int) ((double) getNotFilteredDerivation() / inputSize * 100);
			if (getStrategy() == STRATEGY_CLEAN_DUPL_ALWAYS
					|| (getStrategy() == STRATEGY_CLEAN_DUPL_LARGE_DERIVATION && ratio > getDerivationRatio())) {
				// Clean the duplicates
				setFilteredDerivation(deleteDuplicatedTriples(pool.toString(),
						outputDir, "FILTER_ONLY_HIDDEN", pool.toString()
								+ RDFS_OUTPUT_DIR + "/dir-domain-range-"
								+ executionStep, getFilterFromStep(), true,
						false, true));

				// Remove the not filtered directories
				FileSystem.get(job.getConfiguration())
						.delete(new Path(pool.toString()
								+ RDFS_NOT_FILTERED_DIR), true);
				FileSystem.get(job.getConfiguration()).delete(
						new Path(pool.toString() + OWL_NOT_FILTERED_DIR), true);

				setFilterFromStep(executionStep);
				setHasDerived(getFilteredDerivation() > 0);
				return;
			}

			setHasDerived(true);
			return;
		} else {
			FileSystem.get(job.getConfiguration()).delete(new Path(outputDir),
					true);
		}
	}
}
