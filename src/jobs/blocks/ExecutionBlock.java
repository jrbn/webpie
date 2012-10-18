package jobs.blocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import jobs.Reasoner;
import mappers.rdfs.SwapTriplesMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import partitioners.MyHashPartitioner;
import partitioners.TriplesPartitioner;
import readers.FilesTriplesReader;
import reducers.io.CleanDuplicatesReducer;
import utils.FileUtils;
import writers.FilesTriplesWriter;
import data.Triple;
import data.TripleSource;

abstract public class ExecutionBlock {

	public static final String RDFS_OUTPUT_DIR = "/dir-rdfs-output";
	public static final String OWL_OUTPUT_DIR = "/dir-owl-output";
	public static final String OWL_NOT_FILTERED_DIR = "/dir-owl-not-filtered";
	public static final String RDFS_NOT_FILTERED_DIR = "/dir-rdfs-not-filtered";

	public static final String OWL_PROP_INHERITANCE_TMP = OWL_NOT_FILTERED_DIR
			+ "/dir-tmp-prop-inheritance";
	public static final String OWL_TRANSITIVITY_BASE = "/dir-transitivity-base";

	public static final String OWL_SYNONYMS_TABLE = "/dir-table-synonyms";
	public static final String OWL_SYNONYMS_TABLE_NEW = "/_table_synonyms_new";
	public static final String OWL_SAME_AS_INHERITANCE_TMP = OWL_NOT_FILTERED_DIR
			+ "/dir-tmp-same-as-inheritance";

	public static final String OWL_ALL_VALUE_TMP = OWL_NOT_FILTERED_DIR
			+ "/dir-tmp-all-some-values";
	public static final String OWL_HAS_VALUE_TMP = OWL_NOT_FILTERED_DIR
			+ "/dir-tmp-has-value";

	static public int STRATEGY_CLEAN_DUPL_ALWAYS = 0;
	static public int STRATEGY_CLEAN_DUPL_LARGE_DERIVATION = 1;
	static public int STRATEGY_CLEAN_DUPL_END = 2;

	static public int FRAGMENT_RDFS = 0;
	static public int FRAGMENT_OWL = 1;
	static public int FRAGMENT_RDFSOWL = 2;
	static public int FRAGMENT_OWL2 = 3;
	static public int FRAGMENT_RDFSOWL2 = 4;
	static public int FRAGMENT_ONLY_OWL_SAMEAS = 5;

	protected int numMapTasks = 1;
	protected int numReduceTasks = 1;
	private boolean hasDerived = false;
	protected Configuration conf = null;
	private boolean neverExecuted = true;
	private long currentNotFilteredDerivation = 0;
	private long currentFilteredDerivation = 0;
	Path pool = null;
	private int strategy = STRATEGY_CLEAN_DUPL_ALWAYS;
	private int derivationRatio = 0;
	private int previousExecution = -1;
	private int currentExecution = -1;
	private int filterFromStep = -1;

	public int getFilterFromStep() {
		return filterFromStep;
	}

	protected void setFilterFromStep(int filterFromStep) {
		this.filterFromStep = filterFromStep;
	}

	public int getSampling() {
		return sampling;
	}

	public void setSampling(int sampling) {
		this.sampling = sampling;
	}

	public int getResourceThreshold() {
		return resourceThreshold;
	}

	public void setResourceThreshold(int resourceThreshold) {
		this.resourceThreshold = resourceThreshold;
	}

	private int sampling = 10;
	private int resourceThreshold = 1000;

	public int getStrategy() {
		return strategy;
	}

	public boolean isHasDerived() {
		return hasDerived;
	}

	public void setHasDerived(boolean hasDerived) {
		this.hasDerived = hasDerived;
	}

	public long getNotFilteredDerivation() {
		return currentNotFilteredDerivation;
	}

	public void setNotFilteredDerivation(long currentNotFilteredDerivation) {
		this.currentNotFilteredDerivation = currentNotFilteredDerivation;
	}

	public long getFilteredDerivation() {
		return currentFilteredDerivation;
	}

	public void setFilteredDerivation(long currentFilteredDerivation) {
		this.currentFilteredDerivation = currentFilteredDerivation;
	}

	public int getPreviousExecution() {
		return previousExecution;
	}

	abstract void performJobs(int executionStep) throws Exception;

	public boolean isNeverExecuted() {
		return neverExecuted;
	}

	protected Job getNewJob(String jobName, String baseInputDir,
			String filenameFilter) throws IOException {
		// Configuration conf = new Configuration(); -- It already has this
		conf.setInt("maptasks", numMapTasks);
		conf.set("input.filter", filenameFilter);
		conf.setInt("reasoner.step", currentExecution);
		conf.setInt("reasoner.previousStep", previousExecution);
		conf.setBoolean("reasoner.neverExecuted", neverExecuted);

		Job job = new Job(conf);
		job.setJobName(jobName);
		job.setJarByClass(ExecutionBlock.class);
		FilesTriplesReader.addInputPath(job, new Path(baseInputDir));
		job.setInputFormatClass(FilesTriplesReader.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setPartitionerClass(MyHashPartitioner.class);

		return job;
	}

	protected void configureOutputJob(Job job, String outputName) {
		// Set the output
		job.setOutputKeyClass(TripleSource.class);
		job.setOutputValueClass(Triple.class);
		job.setOutputFormatClass(FilesTriplesWriter.class);
		Path outputFolder = new Path(outputName);
		FilesTriplesWriter.setOutputPath(job, outputFolder);
	}

	protected long deleteDuplicatedTriples(String inputDir, String tmpDir,
			String inputFilter, String outputDir, int filterStep,
			boolean deleteSource, boolean setStep, boolean derivationKey)
			throws IOException, InterruptedException, ClassNotFoundException {

		Job job = getNewJob("OWL reasoner: cleanup duplicated statements",
				inputDir, inputFilter);

		job.setMapperClass(SwapTriplesMapper.class);
		job.setMapOutputKeyClass(Triple.class);
		job.setMapOutputValueClass(TripleSource.class);
		job.setPartitionerClass(TriplesPartitioner.class);
		job.setReducerClass(CleanDuplicatesReducer.class);
		job.getConfiguration().setInt("reasoner.filterStep", filterStep);
		job.getConfiguration().setBoolean("reasoner.filterSetStep", setStep);
		job.getConfiguration().setBoolean("reasoner.setDerivationKey",
				derivationKey);
		configureOutputJob(job, outputDir);
		job.waitForCompletion(true);

		if (tmpDir != null && deleteSource)
			FileSystem.get(job.getConfiguration()).delete(new Path(tmpDir),
					true);

		Counter filteredTriples = job.getCounters().findCounter(
				"org.apache.hadoop.mapred.Task$Counter",
				"REDUCE_OUTPUT_RECORDS");
		return filteredTriples.getValue();
	}

	public void execute(int executionStep, int filterFromStep) throws Exception {
		currentExecution = executionStep;
		currentNotFilteredDerivation = 0;
		currentFilteredDerivation = 0;
		hasDerived = false;
		this.filterFromStep = filterFromStep;

		// Execute the job
		performJobs(executionStep);
		checkSynonyms(executionStep);
		neverExecuted = false;
		previousExecution = executionStep + 1;
	}

	private void checkSynonyms(int executionStep) throws IOException {
		// Check if there is synonyms table I move it to the root folder
		FileSystem fs = FileSystem.get(this.conf);
		List<Path> toProcess = new ArrayList<Path>();
		toProcess.add(pool);
		boolean found = false;
		Path toMove = null;
		while (!toProcess.isEmpty() && !found) {
			Path dir = toProcess.remove(toProcess.size() - 1);
			FileStatus[] children = fs.listStatus(dir,
					FileUtils.FILTER_SAME_AS_TABLE);
			for (FileStatus child : children) {
				if (child.getPath().getName().equals("dir-synonymstable")) {
					found = true;
					toMove = child.getPath();
				} else if (!child.getPath().getName()
						.equals(Reasoner.OWL_SYNONYMS_TABLE)) {
					toProcess.add(child.getPath());
				}
			}
		}

		if (found) {
			Path parent = new Path(pool, jobs.Reasoner.OWL_SYNONYMS_TABLE);
			if (!fs.exists(parent)) {
				fs.mkdirs(parent);
			}
			Path newSynTableDir = new Path(parent, "dir-output-step-"
					+ executionStep);
			fs.rename(toMove, newSynTableDir);
		}
	}

	public void init(Configuration conf, Path pool, int strategy,
			int derivationRatio, int numMaps, int numReducers, int sampling,
			int resourceThreshold, int previousExecution) {
		this.pool = pool;
		numMapTasks = numMaps;
		numReduceTasks = numReducers;
		this.conf = conf;
		this.strategy = strategy;
		this.derivationRatio = derivationRatio;
		this.sampling = sampling;
		this.resourceThreshold = resourceThreshold;
		this.previousExecution = previousExecution;
	}

	public int getDerivationRatio() {
		return derivationRatio;
	}

	public void setDerivationRatio(int derivationRatio) {
		this.derivationRatio = derivationRatio;
	}

	public boolean hasDerived() {
		return hasDerived;
	}
}
