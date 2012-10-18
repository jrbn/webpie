package jobs;

import java.io.IOException;

import mappers.io.ExportTriplesDecontructMapper;
import mappers.io.ExportTriplesJoinDictMapper;
import mappers.io.ExportTriplesReconstructMapper;
import mappers.io.ExportTriplesSampleMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitioners.MyHashPartitioner;
import readers.FilesCombinedReader;
import readers.FilesDictReader;
import readers.FilesTriplesReader;
import reducers.io.ExportTriplesDeconstructReducer;
import reducers.io.ExportTriplesReconstructReducer;
import reducers.io.ExportTriplesSampleReducer;

public class FilesExportTriples extends Configured implements Tool {

	private static Logger log = LoggerFactory
			.getLogger(FilesExportTriples.class);

	// Parameters
	private int numMapTasks = 4;
	private int numReduceTasks = 2;

	private int sampling = 0;
	private int resourceThreshold = 0;

	private void parseArgs(String[] args) {

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
		}
	}

	// Create new job and set all parameters
	private Job createNewJob(String name) throws IOException {
		Configuration conf = new Configuration(getConf());
		conf.setInt("reasoner.samplingPercentage", sampling);
		conf.setInt("reasoner.threshold", resourceThreshold);
		conf.setInt("maptasks", numMapTasks);
		conf.set("input.filter", "FILTER_ONLY_HIDDEN");
		conf.setBoolean("mapred.compress.map.output", true);

		Job job = new Job(conf);
		job.setJarByClass(FilesExportTriples.class);
		job.setJobName(name);
		job.setNumReduceTasks(numReduceTasks);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job,
				CompressionType.BLOCK);

		return job;
	}

	private void sampleCommonResources(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = createNewJob("Sampling common resources");

		// Input
		FilesTriplesReader.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(FilesTriplesReader.class);

		// Job
		job.setMapperClass(ExportTriplesSampleMapper.class);
		job.setPartitionerClass(MyHashPartitioner.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(ExportTriplesSampleReducer.class);

		// Output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		Path commonResourcesPath = new Path(new Path(args[1]),
				"_commonResources");
		SequenceFileOutputFormat.setOutputPath(job, commonResourcesPath);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Launch
		long time = System.currentTimeMillis();
		job.waitForCompletion(true);
		log.info("Job finished in " + (System.currentTimeMillis() - time));
	}

	private void joinCommonResources(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = createNewJob("Join common resources");
		job.getConfiguration().set("commonResources",
				args[1] + "/_commonResources");

		// Input
		job.setInputFormatClass(FilesDictReader.class);
		SequenceFileInputFormat.addInputPath(job, new Path(args[0],
				"_dict/table"));

		// Job
		job.setMapperClass(ExportTriplesJoinDictMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setNumReduceTasks(0);

		// Output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1],
				"_joinCommonResources"));

		// Launch
		long time = System.currentTimeMillis();
		job.waitForCompletion(true);
		log.info("Job finished in " + (System.currentTimeMillis() - time));
	}

	private long fullDictionaryDecoding(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = createNewJob("Dictionary decoding");
		job.getConfiguration().set("joinCommonResources",
				args[1] + "/_joinCommonResources");

		// Input
		job.setInputFormatClass(FilesCombinedReader.class);
		SequenceFileInputFormat.addInputPath(job, new Path(args[0],
				"_dict/table"));
		SequenceFileInputFormat.addInputPath(job, new Path(args[0]));

		// Job
		job.setMapperClass(ExportTriplesDecontructMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setPartitionerClass(MyHashPartitioner.class);
		job.setReducerClass(ExportTriplesDeconstructReducer.class);

		// Output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1],
				"deconstructedTriples"));

		// Launch
		long time = System.currentTimeMillis();
		job.waitForCompletion(true);
		log.info("Job finished in " + (System.currentTimeMillis() - time));
		return 0;
	}

	private long reconstructTriples(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = createNewJob("Reconstruct triples");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPathFilter(job, OutputLogFilter.class);
		SequenceFileInputFormat.addInputPath(job, new Path(args[1],
				"deconstructedTriples"));

		job.setMapperClass(ExportTriplesReconstructMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setPartitionerClass(MyHashPartitioner.class);
		job.setReducerClass(ExportTriplesReconstructReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1], "triples"));

		// Launch
		long time = System.currentTimeMillis();
		job.waitForCompletion(true);
		log.info("Job finished in " + (System.currentTimeMillis() - time));

		// Clean the intermediate directories
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(new Path(args[1], "_commonResources"), true);
		fs.delete(new Path(args[1], "_joinCommonResources"), true);
		fs.delete(new Path(args[1], "deconstructedTriples"), true);

		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		try {
			parseArgs(args);
			FileSystem.get(getConf()).delete(new Path(args[1]), true);
			sampleCommonResources(args);
			joinCommonResources(args);
			fullDictionaryDecoding(args);
			reconstructTriples(args);
		} catch (Exception e) {
			log.error("Error in the execution: " + e);
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: ExportTriples [input dir] [output dir]");
			System.exit(0);
		}

		long time = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new FilesExportTriples(),
				args);
		log.info("Export time: " + (System.currentTimeMillis() - time));
		System.exit(res);
	}
}
