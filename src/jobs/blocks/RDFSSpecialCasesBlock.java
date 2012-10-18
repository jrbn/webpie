package jobs.blocks;

import java.io.IOException;

import mappers.rdfs.RDFSSpecialPropsMapper;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import reducers.rdfs.RDFSSpecialPropsReducer;

public class RDFSSpecialCasesBlock extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		if (!conf.getBoolean("ShouldCalculateRDFSSpecialCases", false)) {
			return;
		}

		Job job = getNewJob("RDFS special cases. Step " + executionStep,
				pool.toString(), "FILTER_ONLY_OTHER_SUBCLASS_SUBPROP");

		job.setMapperClass(RDFSSpecialPropsMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(RDFSSpecialPropsReducer.class);

		String outputDir = pool.toString() + RDFS_OUTPUT_DIR
				+ "/dir-special-props-" + executionStep;
		configureOutputJob(job, outputDir);
		job.waitForCompletion(true);
		setFilteredDerivation(job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"REDUCE_OUTPUT_RECORDS").getValue());
		setNotFilteredDerivation(getFilteredDerivation());

		setHasDerived(getFilteredDerivation() > 0);
	}
}
