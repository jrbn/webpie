package jobs.blocks;

import java.io.IOException;
import java.util.List;

import mappers.owl.OWLChangeDerivationTransitivity;
import mappers.owl.OWLExtractTransitivityTriples;
import mappers.owl.OWLTransitivityMapper;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;

import partitioners.MyHashPartitioner;
import readers.FilesTriplesReader;
import readers.MultiFilesReader;
import reducers.owl.OWLTransitivityReducer;
import writers.FilesTriplesWriter;
import data.Triple;
import data.TripleSource;

public class OWLTransitivityBlock extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		boolean derivedNewStatements = true;
		FileSystem fs = FileSystem.get(conf);

		// Check if there are transitivity properties.
		Job job = new Job(conf);
		FilesTriplesReader.addInputPath(job, pool);
		List<FileStatus> files = MultiFilesReader.recursiveListStatus(job,
				"FILTER_ONLY_OWL_TRANSITIVE_SCHEMA");
		if (files == null || files.size() == 0) {
			return;
		}

		// Launch a job which extract the transitive triples
		job.setJarByClass(OWLTransitivityBlock.class);
		job.setJobName("OWL reasoner: extract transitive triples "
				+ executionStep);
		job.setInputFormatClass(FilesTriplesReader.class);
		job.getConfiguration().setInt("reasoner.previousExecution",
				getPreviousExecution());
		job.getConfiguration().setInt("reasoner.currentExecution",
				executionStep);
		job.getConfiguration().setInt("maptasks", numMapTasks);
		job.setNumReduceTasks(0);
		job.setMapperClass(OWLExtractTransitivityTriples.class);
		job.setMapOutputKeyClass(TripleSource.class);
		job.setMapOutputValueClass(Triple.class);
		job.setOutputFormatClass(FilesTriplesWriter.class);
		Path transitivityFolder = new Path(pool.toString()
				+ OWL_NOT_FILTERED_DIR + OWL_TRANSITIVITY_BASE);
		FilesTriplesWriter.setOutputPath(job, new Path(transitivityFolder,
				"dir-level-0"));
		job.waitForCompletion(true);
		long newTriples = job.getCounters()
				.findCounter("OWL derived triples", "new transitivity triples")
				.getValue();
		if (newTriples == 0) {
			fs.delete(transitivityFolder, true);
			return;
		}

		int level = 0;
		long transitivityDerivation = 0;
		while (derivedNewStatements) {
			job = new Job(conf);
			job.setJarByClass(OWLTransitivityBlock.class);
			job.getConfiguration().setInt("reasoning.baseLevel", executionStep);
			level++;
			job.getConfiguration().setInt("reasoning.transitivityLevel", level);
			job.setJobName("OWL reasoner: transitivity rule. Level " + level);
			if (level < 3) {
				FilesTriplesReader.addInputPath(job, new Path(
						transitivityFolder + "/dir-level-0/"));
			}

			if (level > 1) { // Add the directory previous level
				FilesTriplesReader
						.addInputPath(job, new Path(transitivityFolder
								+ "/dir-level-" + (level - 1) + "/"));
			}

			if (level > 2) { // Add also the directory level - 2
				FilesTriplesReader
						.addInputPath(job, new Path(transitivityFolder
								+ "/dir-level-" + (level - 2) + "/"));
			}
			job.setInputFormatClass(FilesTriplesReader.class);
			job.getConfiguration().setInt("maptasks", numMapTasks);
			job.setNumReduceTasks(numReduceTasks);

			job.setMapperClass(OWLTransitivityMapper.class);
			job.setMapOutputKeyClass(BytesWritable.class);
			job.setMapOutputValueClass(BytesWritable.class);
			job.setPartitionerClass(MyHashPartitioner.class);
			job.setReducerClass(OWLTransitivityReducer.class);

			// Configure output
			job.setOutputKeyClass(TripleSource.class);
			job.setOutputValueClass(Triple.class);
			job.setOutputFormatClass(FilesTriplesWriter.class);
			Path outputFolder = new Path(transitivityFolder,
					"dir-not-filtered-level-" + level + "/");
			FilesTriplesWriter.setOutputPath(job, outputFolder);
			job.waitForCompletion(true);

			long stepNotFilteredDerivation = job
					.getCounters()
					.findCounter("org.apache.hadoop.mapred.Task$Counter",
							"REDUCE_OUTPUT_RECORDS").getValue();

			setNotFilteredDerivation(getNotFilteredDerivation()
					+ stepNotFilteredDerivation);
			long stepDerivation = 0;
			if (stepNotFilteredDerivation > 0) {
				stepDerivation = deleteDuplicatedTriples(
						transitivityFolder.toString(), outputFolder.toString(),
						"FILTER_ONLY_HIDDEN", new Path(transitivityFolder,
								"dir-level-" + level).toString(),
						Math.max(1, (int) Math.pow(2, level - 1))
								+ executionStep - 1, true, false, true);
			}
			transitivityDerivation += stepDerivation;
			derivedNewStatements = stepDerivation > 0;
		}

		// Remove derivation step
		setFilteredDerivation(transitivityDerivation);
		if (transitivityDerivation > 0) {
			job = new Job(conf);
			job.setJarByClass(OWLTransitivityBlock.class);
			job.setJobName("OWL reasoner: change derivation step to "
					+ executionStep);
			FilesTriplesReader.addInputPath(job, transitivityFolder);
			job.setInputFormatClass(FilesTriplesReader.class);
			job.getConfiguration().setInt("reasoner.currentExecution",
					executionStep);
			job.getConfiguration().setInt("maptasks", numMapTasks);
			job.setNumReduceTasks(0);
			job.setMapperClass(OWLChangeDerivationTransitivity.class);
			job.setMapOutputKeyClass(TripleSource.class);
			job.setMapOutputValueClass(Triple.class);
			job.setOutputFormatClass(FilesTriplesWriter.class);
			Path output = new Path(pool.toString() + OWL_OUTPUT_DIR
					+ OWL_TRANSITIVITY_BASE + "-" + executionStep);
			FilesTriplesWriter.setOutputPath(job, output);
			job.waitForCompletion(true);
		}
		fs.delete(transitivityFolder, true);
		setHasDerived(transitivityDerivation > 0);
	}
}
