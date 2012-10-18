package jobs.blocks;

import java.io.IOException;

import mappers.owl2.OWL2FilterDuplicateListsMapper;
import mappers.owl2.OWL2JoinFirstRestMapper;
import mappers.owl2.OWL2MergeListsMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import reducers.owl2.OWL2FilterDuplicateListsReducer;
import reducers.owl2.OWL2JoinFirstRestReducer;
import reducers.owl2.OWL2MergeListsReducer;

public class OWL2BuildLists extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = getNewJob(
				"OWL2 reasoner: join rest triples with first triples",
				pool.toString(), "FILTER_ONLY_FIRST_REST");
		job.getConfiguration().setInt("reasoner.currentExecution",
				getPreviousExecution());
		job.setMapperClass(OWL2JoinFirstRestMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(OWL2JoinFirstRestReducer.class);
		Path newPath = new Path(pool.toString() + "/_lists/dir-extract");
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, newPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);

		job.waitForCompletion(true);
		long derivation = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"REDUCE_OUTPUT_RECORDS").getValue();

		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (derivation == 0) {
			fs.delete(newPath, true);
			return;
		}

		boolean mergedLists = true;
		int step = 0;
		Path previousPath = new Path(pool.toString() + "/_lists/dir-previous");
		Path currentPath = new Path(pool.toString() + "/_lists/dir-current");
		Path filteredPath = new Path(pool.toString() + "/_lists/dir-filtered");
		derivation = 0;
		while (mergedLists) {
			if (fs.exists(currentPath)) {
				fs.rename(currentPath, previousPath);
			}
			job = new Job();
			job.getConfiguration().setInt("maptasks", numMapTasks);
			job.setJobName("Merge RDF lists. Step " + ++step);
			job.setJarByClass(OWL2BuildLists.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPathFilter(job,
					OutputLogFilter.class);
			if (fs.exists(newPath)) {
				SequenceFileInputFormat.addInputPath(job, newPath);
			}
			if (fs.exists(previousPath)) {
				SequenceFileInputFormat.addInputPath(job, previousPath);
			}
			job.setMapperClass(OWL2MergeListsMapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(BytesWritable.class);
			job.setReducerClass(OWL2MergeListsReducer.class);
			job.setOutputKeyClass(BytesWritable.class);
			job.setOutputValueClass(BytesWritable.class);

			job.setNumReduceTasks(numReduceTasks);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputPath(job, currentPath);

			job.waitForCompletion(true);
			fs.delete(previousPath, true);
			fs.delete(newPath, true);
			long merges = job.getCounters().findCounter("reasoner", "merge")
					.getValue();
			mergedLists = merges > 0;
			derivation += merges;
		}

		if (derivation > 0) {
			// Filter the duplicates
			job = new Job();
			job.getConfiguration().setInt("maptasks", numMapTasks);
			job.setJobName("Filter duplicated lists " + step);
			job.setJarByClass(OWL2BuildLists.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPathFilter(job,
					OutputLogFilter.class);
			SequenceFileInputFormat.addInputPath(job, currentPath);
			job.setMapperClass(OWL2FilterDuplicateListsMapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(BytesWritable.class);
			job.setReducerClass(OWL2FilterDuplicateListsReducer.class);
			job.setOutputKeyClass(BytesWritable.class);
			job.setOutputValueClass(BytesWritable.class);

			job.setNumReduceTasks(numReduceTasks);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputPath(job, filteredPath);

			job.waitForCompletion(true);
			fs.delete(currentPath, true);
			fs.rename(filteredPath, currentPath);
		}

		setFilteredDerivation(derivation);
		setHasDerived(derivation > 0);
	}
}
