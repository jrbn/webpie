package jobs.blocks;

import java.io.IOException;

import mappers.rdfs.RDFSSubclasMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import reducers.rdfs.RDFSSubclasReducer;

public class RDFSSubclassBlock extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = getNewJob("RDFS subclass reasoning. Step " + executionStep,
				pool.toString(), "FILTER_ONLY_TYPE_SUBCLASS");

		job.setMapperClass(RDFSSubclasMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(RDFSSubclasReducer.class);
		String outputDir = pool.toString() + RDFS_OUTPUT_DIR + "/dir-subclass-"
				+ executionStep;
		configureOutputJob(job, outputDir);
		job.waitForCompletion(true);
		setFilteredDerivation(job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"REDUCE_OUTPUT_RECORDS").getValue());
		setNotFilteredDerivation(getFilteredDerivation());

		// Check whether it makes sense to launch special block
		long specialTriples = 0;
		Counter counter = job.getCounters().findCounter("RDFS derived triples",
				"subclass of Literal");
		specialTriples += counter.getValue();
		counter = job.getCounters().findCounter("RDFS derived triples",
				"subproperty of member");
		specialTriples += counter.getValue();
		conf.setBoolean("ShouldCalculateRDFSSpecialCases", specialTriples > 0);

		// Remove the directory if empty
		if (getFilteredDerivation() == 0) {
			FileSystem.get(job.getConfiguration()).delete(new Path(outputDir),
					true);
		} else {
			setHasDerived(true);
		}
	}
}
