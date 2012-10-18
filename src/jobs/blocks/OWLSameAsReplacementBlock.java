package jobs.blocks;

import java.io.IOException;

import jobs.Reasoner;
import mappers.owl.OWLSameAsDeconstructMapper;
import mappers.owl.OWLSameAsReconstructMapper;
import mappers.owl.OWLSampleResourcesMapper;
import mappers.rdfs.SwapTriplesMapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import partitioners.MyHashPartitioner;
import partitioners.SameAsPartitioner;
import readers.FilesTriplesReader;
import reducers.io.CleanDuplicatesReducer;
import reducers.owl.OWLSameAsDeconstructReducer;
import reducers.owl.OWLSameAsReconstructReducer;
import reducers.owl.OWLSampleResourcesReducer;
import data.Triple;
import data.TripleSource;

public class OWLSameAsReplacementBlock extends ExecutionBlock {

    @Override
    public void performJobs(int executionStep) throws IOException,
	    InterruptedException, ClassNotFoundException {

	if (!conf.getBoolean("ShouldRunSameAsInheritance", false)) {
	    return;
	} else {
	    conf.setBoolean("ShouldRunSameAsInheritance", false);
	}

	String outputSynonymsDir = pool.toString() + OWL_NOT_FILTERED_DIR
		+ "/dir-rewritten-triples-" + executionStep;
	String outputSynonymsDirFiltered = pool.toString()
		+ OWL_NOT_FILTERED_DIR + "/dir-rewritten-filtered-triples-"
		+ executionStep;

	// 1) Calculate the URIs distribution and get the first 2M.
	conf.set("input.filter", "FILTER_ONLY_HIDDEN");

	Job job = new Job(conf);
	job.setJarByClass(Reasoner.class);
	job.getConfiguration().setInt("maptasks", numMapTasks);
	job.setNumReduceTasks(numReduceTasks);
	job.setJobName("OWL reasoner: sampling more common resources");
	job.getConfiguration().setInt("reasoner.samplingPercentage",
		getSampling());
	job.getConfiguration().setInt("reasoner.threshold",
		getResourceThreshold());
	job.setInputFormatClass(FilesTriplesReader.class);
	FilesTriplesReader.addInputPath(job, pool);

	job.setMapperClass(OWLSampleResourcesMapper.class);
	job.setPartitionerClass(MyHashPartitioner.class);
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(LongWritable.class);
	job.setReducerClass(OWLSampleResourcesReducer.class);

	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(LongWritable.class);

	Path commonResourcesPath = new Path(pool, "_commonResources");
	SequenceFileOutputFormat.setOutputPath(job, commonResourcesPath);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputCompressionType(job,
		CompressionType.BLOCK);
	job.waitForCompletion(true);

	// 2) Launch a job that split the triples
	job = new Job(conf);
	job.setJarByClass(Reasoner.class);
	job.getConfiguration().setInt("maptasks", numMapTasks);
	job.setNumReduceTasks(numReduceTasks);

	job.setJobName("OWL reasoner: replace triples using the sameAs synonyms: deconstruct triples");
	job.setInputFormatClass(FilesTriplesReader.class);
	FilesTriplesReader.addInputPath(job, pool);

	job.setMapperClass(OWLSameAsDeconstructMapper.class);
	job.setPartitionerClass(SameAsPartitioner.class);
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(BytesWritable.class);
	job.setReducerClass(OWLSameAsDeconstructReducer.class);

	Path tmpPath = new Path(pool, "deconstructTriples");
	SequenceFileOutputFormat.setOutputPath(job, tmpPath);
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(BytesWritable.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	SequenceFileOutputFormat.setOutputCompressionType(job,
		CompressionType.BLOCK);
	job.waitForCompletion(true);

	// 3) Launch a job that reconstruct the triples
	job = new Job(conf);
	job.setJarByClass(Reasoner.class);
	job.getConfiguration().setInt("maptasks", numMapTasks);
	job.setNumReduceTasks(numReduceTasks);

	job.setJobName("OWL reasoner: replace triples using the sameAs synonyms: reconstruct triples");
	SequenceFileInputFormat.addInputPath(job, tmpPath);
	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setMapperClass(OWLSameAsReconstructMapper.class);
	job.setMapOutputKeyClass(BytesWritable.class);
	job.setMapOutputValueClass(BytesWritable.class);
	job.setReducerClass(OWLSameAsReconstructReducer.class);
	configureOutputJob(job, outputSynonymsDir);
	job.waitForCompletion(true);

	FileSystem fs = FileSystem.get(job.getConfiguration());
	fs.delete(tmpPath, true);
	fs.delete(commonResourcesPath, true);

	// Launch a job that cleans up the duplicates
	if (getStrategy() != STRATEGY_CLEAN_DUPL_END) {
	    job = getNewJob("OWL reasoner: cleanup duplicated statements",
		    outputSynonymsDir, "FILTER_ONLY_HIDDEN");
	    job.setMapperClass(SwapTriplesMapper.class);
	    job.setMapOutputKeyClass(Triple.class);
	    job.setMapOutputValueClass(TripleSource.class);
	    job.setReducerClass(CleanDuplicatesReducer.class);
	    configureOutputJob(job, outputSynonymsDirFiltered);
	    job.waitForCompletion(true);
	    setFilterFromStep(executionStep);
	} else {
	    fs.rename(new Path(outputSynonymsDir), new Path(
		    outputSynonymsDirFiltered));
	}

	// Remove all the others directories. Keep only the last one
	// produced.
	if (fs.exists(new Path(pool, "dir-input")))
	    fs.rename(new Path(pool, "dir-input"), new Path(pool, "_dir-input"));

	fs.delete(new Path(pool.toString() + RDFS_OUTPUT_DIR), true);
	fs.delete(new Path(pool.toString() + OWL_OUTPUT_DIR), true);
	fs.mkdirs(new Path(pool.toString() + OWL_OUTPUT_DIR));
	fs.rename(new Path(outputSynonymsDirFiltered), new Path(pool.toString()
		+ OWL_OUTPUT_DIR + "/dir-rewrite-sameas-step-" + executionStep));
	fs.delete(new Path(pool.toString() + OWL_NOT_FILTERED_DIR), true);

	return;
    }

    @Override
    public long getFilteredDerivation() {
	return 0;
    }

    @Override
    public long getNotFilteredDerivation() {
	return 0;
    }
}
