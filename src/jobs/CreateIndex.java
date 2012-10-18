package jobs;

import mappers.io.CreateIndexMapper;
import mappers.io.SamplePartitionDistrMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitioners.BalancedPartitioner;
import readers.FilesTriplesReader;
import reducers.io.CreateIndexReducer;
import reducers.io.SamplePartitionDistrReducer;
import writers.IndexWriter2;
import writers.SampleIndexWriter;

public class CreateIndex extends Configured implements Tool {

    public static final String[] indices = new String[] { "spo", "sop", "pos",
	    "pso", "osp", "ops" };

    public final int DEFAULT_CHUNK_SIZE = 1024 * 1024 * 32; // 32MB

    private static Logger log = LoggerFactory.getLogger(CreateIndex.class);
    private int numReduceTasks = 1;
    private int samplingPercentage = 100;
    private long estimatedSampleSize = 0;
    private int sizeChunk = 1;

    public void parseArgs(String[] args) {
	for (int i = 0; i < args.length; ++i) {
	    if (args[i].equalsIgnoreCase("--reducetasks")) {
		numReduceTasks = Integer.valueOf(args[++i]);
	    }

	    if (args[i].equalsIgnoreCase("--samplingPercentage")) {
		samplingPercentage = Integer.valueOf(args[++i]);
	    }

	    if (args[i].equalsIgnoreCase("--estimatedSampleSize")) {
		estimatedSampleSize = Long.valueOf(args[++i]);
	    }

	    if (args[i].equalsIgnoreCase("--sizeIndexChunk")) {
		sizeChunk = Integer.valueOf(args[++i]);
	    }
	}
    }

    public static void main(String[] args) throws Exception {

	if (args.length < 2) {
	    System.out.println("Usage: CreateIndex [input dir] [output dir]");
	    System.exit(0);
	}

	long time = System.currentTimeMillis();
	int res = ToolRunner.run(new Configuration(), new CreateIndex(), args);
	log.info("Create index time: " + (System.currentTimeMillis() - time));
	System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
	parseArgs(args);

	Job job = new Job(getConf());
	job.setJobName("Sampling to calculate triples distribution");
	job.setJarByClass(CreateIndex.class);
	job.getConfiguration().setInt("samplingPercentage", samplingPercentage);
	job.getConfiguration().setInt("nPartitions", numReduceTasks);
	job.getConfiguration().setLong("estimatedSampleSize",
		estimatedSampleSize);
	job.setInputFormatClass(FilesTriplesReader.class);
	FilesTriplesReader.addInputPath(job, new Path(args[0]));
	job.setMapperClass(SamplePartitionDistrMapper.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(SamplePartitionDistrReducer.class);
	job.setOutputKeyClass(BytesWritable.class);
	job.setOutputValueClass(NullWritable.class);
	job.setOutputFormatClass(SampleIndexWriter.class);
	SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);

	job = new Job(getConf());
	job.setJobName("Create index");
	job.setJarByClass(CreateIndex.class);
	job.setInputFormatClass(FilesTriplesReader.class);
	FilesTriplesReader.addInputPath(job, new Path(args[0]));
	job.setMapperClass(CreateIndexMapper.class);
	job.setMapOutputKeyClass(BytesWritable.class);
	job.setMapOutputValueClass(NullWritable.class);
	job.getConfiguration().set("partitionsLocation", args[1]);
	job.setNumReduceTasks(numReduceTasks);
	job.setPartitionerClass(BalancedPartitioner.class);
	job.setReducerClass(CreateIndexReducer.class);
	job.setOutputKeyClass(BytesWritable.class);
	job.setOutputValueClass(NullWritable.class);
	job.getConfiguration().setInt("sizeChunk", sizeChunk);
	job.setOutputFormatClass(IndexWriter2.class);
	SequenceFileOutputFormat.setOutputCompressionType(job,
		CompressionType.BLOCK);
	SequenceFileOutputFormat.setCompressOutput(job, true);
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);
	// } else {
	// // Partition them according to the hash of the triple
	// Job job = new Job(getConf());
	// job.setJobName("Create index " + index + "(partition by hash)");
	// job.setJarByClass(CreateIndex.class);
	// job.setInputFormatClass(FilesTriplesReader.class);
	// FilesTriplesReader.addInputPath(job, new Path(args[0]));
	// job.setMapperClass(CreateIndexMapper.class);
	// job.setMapOutputKeyClass(BytesWritable.class);
	// job.setMapOutputValueClass(NullWritable.class);
	// job.getConfiguration().set("indexType", index);
	// job.setNumReduceTasks(numReduceTasks);
	// job.setPartitionerClass(BalancedPartitioner2.class);
	// job.setReducerClass(CreateIndexReducer.class);
	// job.setOutputKeyClass(BytesWritable.class);
	// job.setOutputValueClass(NullWritable.class);
	// job.getConfiguration().setInt("sizeChunk", sizeChunk);
	// // job.setOutputFormatClass(IndexWriter.class); // The files are
	// // // sequence
	// // // files
	// job.setOutputFormatClass(IndexWriter2.class); // The files are
	// // normal binary
	// // compressed
	// // files
	// SequenceFileOutputFormat.setOutputCompressionType(job,
	// CompressionType.BLOCK);
	// SequenceFileOutputFormat.setCompressOutput(job, true);
	// FileOutputFormat.setOutputPath(job, new Path(args[1] + "/"
	// + index, "index"));
	// job.waitForCompletion(true);
	// }
	// }

	return 0;
    }
}
