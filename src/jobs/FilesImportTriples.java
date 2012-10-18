package jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mappers.io.ImportTriplesDeconstructMapper;
import mappers.io.ImportTriplesReconstructMapper;
import mappers.io.ImportTriplesSampleMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitioners.MyHashPartitioner;
import readers.MultiFilesReader;
import readers.NTriplesCombinedReader;
import reducers.io.ImportTriplesDeconstructReducer;
import reducers.io.ImportTriplesReconstructReducer;
import reducers.io.ImportTriplesSampleReducer;
import utils.FileUtils;
import writers.FilesCombinedWriter;
import writers.FilesDictWriter;
import writers.FilesTriplesWriter;
import data.Triple;
import data.TripleSource;

public class FilesImportTriples extends Configured implements Tool {

	private static Logger log = LoggerFactory
			.getLogger(FilesImportTriples.class);

	// Parameters
	private int numReduceTasks = 2;
	private int numMapTasks = 4;
	private int sampling = 0;
	private int resourceThreshold = 0;
	private int inputStep = 0;
	private boolean rewriteBlankNodes = true;
	private boolean noDictionary = true;

	public int run(String[] args) throws Exception {
		parseArgs(args);
		FileSystem fs = FileSystem.get(this.getConf());
		fs.delete(new Path(args[1]), true);
		sampleCommonResources(args);
		assignIdsToNodes(args);
		rewriteTriples(args);
		fs.delete(new Path(args[1]), true);
		return 0;
	}

	private Job createNewJob(String name) throws IOException {
		Configuration conf = new Configuration(this.getConf());
		conf.setInt("reasoner.samplingPercentage", sampling);
		conf.setInt("reasoner.threshold", resourceThreshold);
		conf.setInt("maptasks", numMapTasks);
		conf.setBoolean("mapred.compress.map.output", true);

		Job job = new Job(conf);
		job.setJarByClass(FilesImportTriples.class);
		job.setJobName(name);
		job.setNumReduceTasks(numReduceTasks);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job,
				CompressionType.BLOCK);

		return job;
	}

	public void parseArgs(String[] args) {

		for (int i = 0; i < args.length; ++i) {

			if (args[i].equalsIgnoreCase("--maptasks")) {
				numMapTasks = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--samplingPercentage")) {
				sampling = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--samplingThreshold")) {
				resourceThreshold = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--inputStep")) {
				inputStep = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--rewriteBlankNodes")) {
				rewriteBlankNodes = Boolean.valueOf(args[++i]);
			}
		}
	}

	public void sampleCommonResources(String[] args) throws Exception {
		Job job = createNewJob("Sample common resources");
		job.getConfiguration().setBoolean("ImportTriples.rewriteBlankNodes",
				rewriteBlankNodes);

		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputPathFilter(job,
				FileUtils.FILTER_ONLY_HIDDEN.getClass());
		// Check if the dictionary exists and if yes add it to the input
		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(new Path(args[2] + "/_dict"))) {
			FileInputFormat.addInputPath(job,
					new Path(args[2] + "/_dict/table"));
			noDictionary = false;
		} else {
			noDictionary = true;
		}
		job.getConfiguration().setBoolean("ImportTriples.noDictionary",
				noDictionary);

		// If there are existing counters then load them in the job
		if (fs.exists(new Path(args[2] + "/_dict/_commonCounters.txt"))) {
			FSDataInputStream fin = fs.open(new Path(args[2]
					+ "/_dict/_commonCounters.txt"));
			InputStreamReader in = new InputStreamReader(fin);
			BufferedReader bin = new BufferedReader(in);

			try {
				String line = null;
				while (true) {
					line = bin.readLine();
					String[] tokens = line.split("\t");
					job.getConfiguration().setInt("counter-" + tokens[0],
							Integer.valueOf(tokens[1]));
				}
			} catch (Exception e) {
			}
			bin.close();
			in.close();
			fin.close();
		}

		MultiFilesReader.setSplitable(job.getConfiguration(), false);
		job.setInputFormatClass(NTriplesCombinedReader.class);

		// Job
		job.setMapperClass(ImportTriplesSampleMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(ImportTriplesSampleReducer.class);

		// Output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[2]
				+ "/_commons"));
		job.setOutputFormatClass(FilesDictWriter.class);

		long time = System.currentTimeMillis();
		job.waitForCompletion(true);
		log.info("Job finished in " + (System.currentTimeMillis() - time));

		// Create a file where I save the current counters
		Map<Integer, Long> counters = new HashMap<Integer, Long>();
		for (int i = 0; i < numReduceTasks; ++i) {
			CounterGroup group = job.getCounters().getGroup("counter-" + i);
			Iterator<Counter> itr = group.iterator();
			while (itr.hasNext()) {
				Counter counter = itr.next();
				long lCounter = Long.valueOf(counter.getName());
				if (lCounter != 0) {
					counters.put(i, lCounter);
				}
			}
		}

		// Print the content of the hashmap in a file
		FSDataOutputStream fout = fs.create(new Path(args[2]
				+ "/_dict/_commonCounters.txt"));
		for (Entry<Integer, Long> entry : counters.entrySet()) {
			String string = entry.getKey() + "\t" + entry.getValue() + "\n";
			fout.write(string.getBytes());
		}
		fout.flush();
		fout.close();
	}

	public void assignIdsToNodes(String[] args) throws Exception {
		Job job = createNewJob("Deconstruct statements");
		job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
		job.getConfiguration().set("commonResources", args[2] + "/_commons");
		job.getConfiguration().setBoolean("ImportTriples.rewriteBlankNodes",
				rewriteBlankNodes);
		job.getConfiguration().setBoolean("ImportTriples.noDictionary",
				noDictionary);

		// If there are existing counters then load them in the job
		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(new Path(args[2] + "/_dict/_otherCounters.txt"))) {
			FSDataInputStream fin = fs.open(new Path(args[2]
					+ "/_dict/_otherCounters.txt"));
			InputStreamReader in = new InputStreamReader(fin);
			BufferedReader bin = new BufferedReader(in);

			try {
				String line = null;
				while (true) {
					line = bin.readLine();
					String[] tokens = line.split("\t");
					job.getConfiguration().setInt("counter-" + tokens[0],
							Integer.valueOf(tokens[1]));
				}
			} catch (Exception e) {
			}
			bin.close();
			in.close();
			fin.close();
		}

		if (fs.exists(new Path(args[2] + "/_dict/table"))) {
			FileInputFormat.addInputPath(job,
					new Path(args[2] + "/_dict/table"));
		}

		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputPathFilter(job,
				FileUtils.FILTER_ONLY_HIDDEN.getClass());
		job.setInputFormatClass(NTriplesCombinedReader.class);
		MultiFilesReader.setSplitable(job.getConfiguration(), false);

		// Job
		job.setMapperClass(ImportTriplesDeconstructMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(ImportTriplesDeconstructReducer.class);

		// Output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(FilesCombinedWriter.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Launch
		long time = System.currentTimeMillis();
		job.waitForCompletion(true);
		log.info("Job finished in " + (System.currentTimeMillis() - time));

		// Move the new commons in the table dir of the directory
		int dirIndex = 0;
		if (fs.exists(new Path(args[2] + "/_dict/table"))) {
			FileStatus[] files = fs.listStatus(new Path(args[2]
					+ "/_dict/table"));
			for (FileStatus file : files) {
				try {
					int fileIndex = Integer.valueOf(file.getPath().getName());
					if (fileIndex > dirIndex) {
						dirIndex = fileIndex;
					}
				} catch (Exception e) {
				}
			}
			dirIndex++;
		} else {
			fs.mkdirs(new Path(args[2] + "/_dict/table"));
		}

		if (fs.exists(new Path(args[2] + "/_commons/new"))) {
			fs.rename(new Path(args[2] + "/_commons/new"), new Path(args[2]
					+ "/_dict/table/" + dirIndex));
		}
		fs.delete(new Path(args[2] + "/_commons/"), true);

		// Move the "other" resources in a new directory in the table
		++dirIndex;
		fs.rename(new Path(args[1] + "/new"), new Path(args[2]
				+ "/_dict/table/" + dirIndex));
		fs.delete(new Path(args[1] + "/old"), true);

		// Create a file where I save the current counters
		Map<Integer, Long> counters = new HashMap<Integer, Long>();
		for (int i = 0; i < numReduceTasks; ++i) {
			CounterGroup group = job.getCounters().getGroup("counter-" + i);
			Iterator<Counter> itr = group.iterator();
			while (itr.hasNext()) {
				Counter counter = itr.next();
				long lCounter = Long.valueOf(counter.getName());
				if (lCounter != 0) {
					counters.put(i, lCounter);
				}
			}
		}

		// Print the content of the hashmap in a file
		FSDataOutputStream fout = fs.create(new Path(args[2]
				+ "/_dict/_otherCounters.txt"));
		for (Entry<Integer, Long> entry : counters.entrySet()) {
			String string = entry.getKey() + "\t" + entry.getValue() + "\n";
			fout.write(string.getBytes());
		}
		fout.flush();
		fout.close();
	}

	private void rewriteTriples(String[] args) throws Exception {
		Job job = createNewJob("Reconstruct statements");
		job.getConfiguration().setInt("inputStep", inputStep);

		// Input
		SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
		SequenceFileInputFormat.setInputPathFilter(job, OutputLogFilter.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// Job
		job.setMapperClass(ImportTriplesReconstructMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setPartitionerClass(MyHashPartitioner.class);
		job.setReducerClass(ImportTriplesReconstructReducer.class);
		job.setOutputKeyClass(TripleSource.class);
		job.setOutputValueClass(Triple.class);

		// Output
		Path outputDir = new Path(args[2], "dir-input");
		FileSystem fs = FileSystem.get(job.getConfiguration());
		int dirIndex = 0;
		if (fs.exists(outputDir)) {
			FileStatus[] files = fs.listStatus(outputDir);
			for (FileStatus file : files) {
				try {
					if (file.getPath().getName().startsWith("dir-incr-")) {
						String index = file
								.getPath()
								.getName()
								.substring(
										file.getPath().getName()
												.lastIndexOf('-') + 1);
						int fileIndex = Integer.valueOf(index);
						if (fileIndex > dirIndex) {
							dirIndex = fileIndex;
						}
					}
				} catch (Exception e) {
				}
			}
			dirIndex++;
			outputDir = new Path(outputDir, "dir-incr-" + dirIndex);
		}
		FilesTriplesWriter.setOutputPath(job, outputDir);
		job.setOutputFormatClass(FilesTriplesWriter.class);

		// Launch
		long time = System.currentTimeMillis();
		job.waitForCompletion(true);
		log.info("Job finished in " + (System.currentTimeMillis() - time));

		// Check if there is synonyms table I move it to the root folder
		Path synTableDir = new Path(outputDir, "dir-synonymstable");
		if (fs.exists(synTableDir)) {
			Path parent = new Path(args[2], Reasoner.OWL_SYNONYMS_TABLE);
			log.info("Found synonyms dir. Move to " + parent);
			if (!fs.exists(parent)) {
				fs.mkdirs(parent);
			}
			Path newSynTableDir = new Path(parent, "dir-input");
			if (fs.exists(newSynTableDir)) {
				FileStatus[] files = fs.listStatus(parent);
				dirIndex = 0;
				for (FileStatus file : files) {
					try {
						if (file.getPath().getName().startsWith("dir-incr-")) {
							String index = file
									.getPath()
									.getName()
									.substring(
											file.getPath().getName()
													.lastIndexOf('-') + 1);
							int fileIndex = Integer.valueOf(index);
							if (fileIndex > dirIndex) {
								dirIndex = fileIndex;
							}
						}
					} catch (Exception e) {
					}
				}
				dirIndex++;
				newSynTableDir = new Path(parent, "dir-incr-" + dirIndex);
			}
			fs.rename(synTableDir, newSynTableDir);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out
					.println("Usage: ImportTriples [input dir] [tmp dir] [output dir]");
			System.exit(0);
		}

		long time = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new FilesImportTriples(),
				args);
		log.info("Import time: " + (System.currentTimeMillis() - time));
		System.exit(res);
	}
}