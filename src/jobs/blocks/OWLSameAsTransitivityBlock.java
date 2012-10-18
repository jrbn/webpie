package jobs.blocks;

import java.io.IOException;

import mappers.owl.OWLSameAsMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import partitioners.MyHashPartitioner;
import reducers.owl.OWLSameAsReducer;

public class OWLSameAsTransitivityBlock extends ExecutionBlock {

	long previousTableSize = 0;
	public long tableSize = 0;

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		boolean derivedSynonyms = true;
		int derivationStep = 1;

		long derivedTriples = 0;
		FileSystem fs = FileSystem.get(conf);
		String inputDir = pool.toString() + OWL_SYNONYMS_TABLE;
		String outputDir = pool.toString() + OWL_SYNONYMS_TABLE_NEW;
		if (!fs.exists(new Path(inputDir))) {
			conf.setBoolean("ShouldRunSameAsInheritance", false);
			return;
		}

		while (derivedSynonyms) {
			Job job = getNewJob(
					"OWL reasoner: build the synonyms table from same as triples - step "
							+ derivationStep++, inputDir,
					"FILTER_ONLY_OWL_SAMEAS");
			job.setMapperClass(OWLSameAsMapper.class);
			job.setPartitionerClass(MyHashPartitioner.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setReducerClass(OWLSameAsReducer.class);

			configureOutputJob(job, outputDir);

			job.waitForCompletion(true);
			Counter cDerivedSynonyms = job.getCounters().findCounter(
					"synonyms", "replacements");
			derivedTriples += cDerivedSynonyms.getValue();
			derivedSynonyms = cDerivedSynonyms.getValue() > 0;

			// Delete the output folder and rename the one just created
			fs.delete(new Path(inputDir), true);
			fs.rename(new Path(outputDir), new Path(inputDir));
		}

		// Filter the table
		tableSize = deleteDuplicatedTriples(inputDir, null,
				"FILTER_ONLY_OWL_SAMEAS", outputDir, -1, false, false, true);
		fs.delete(new Path(inputDir), true);
		fs.rename(new Path(outputDir), new Path(inputDir));

		if (isNeverExecuted() || previousTableSize != tableSize) {
			conf.setBoolean("ShouldRunSameAsInheritance", true);
		} else {
			conf.setBoolean("ShouldRunSameAsInheritance", false);
		}
		previousTableSize = tableSize;

		setFilteredDerivation(derivedTriples);
		setNotFilteredDerivation(derivedTriples);
		setHasDerived(derivedTriples > 0);
	}
}
