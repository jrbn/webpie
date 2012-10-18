package jobs.blocks;

import java.io.IOException;

import mappers.owl.OWLEquivalenceSCSPMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import reducers.owl.OWLEquivalenceSCSPReducer;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

public class OWLEquivalenceBlock extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = getNewJob(
				"OWL reasoner: infer equivalence from subclass and subprop. step "
						+ executionStep, pool.toString(),
				"FILTER_ONLY_SUBCLASS_SUBPROP_EQ_CLASSPROP");
		job.setMapperClass(OWLEquivalenceSCSPMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ProtobufWritable.class);
		job.setReducerClass(OWLEquivalenceSCSPReducer.class);
		String outputDir = pool.toString() + OWL_OUTPUT_DIR
				+ "/dir-transitivity-equivalence-" + executionStep;
		configureOutputJob(job, outputDir);

		job.waitForCompletion(true);
		long derivation = job.getCounters().findCounter(
				"org.apache.hadoop.mapred.Task$Counter",
				"REDUCE_OUTPUT_RECORDS").getValue();
		if (derivation == 0) {
			FileSystem.get(job.getConfiguration()).delete(new Path(outputDir),
					true);
		}

		setNotFilteredDerivation(derivation);
		setFilteredDerivation(derivation);
		setHasDerived(derivation > 0);
	}
}
