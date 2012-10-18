package jobs;

import mappers.io.CreateLUBMDatasetMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitioners.TriplesPartitioner;
import readers.CreateLUBMFakeReader;
import reducers.io.CreateLUBMDatasetReducer;
import writers.FilesTriplesWriter;
import data.Triple;
import data.TripleSource;

public class CreateLUBMDataset extends Configured implements Tool {

	private static Logger log = LoggerFactory
			.getLogger(CreateLUBMDataset.class);
	private int numReduceTasks = -1;
	private int numMapTasks = -1;
	private int uniPerTask = -1;

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Usage: Create100BDataset [output dir]");
			System.exit(0);
		}

		try {
			int res = ToolRunner.run(new Configuration(),
					new CreateLUBMDataset(), args);
			System.exit(res);
		} catch (Exception e) {
			log.error("Error: ", e);
		}
	}

	private void parseArgs(String[] args) {

		for (int i = 0; i < args.length; ++i) {

			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--maptasks")) {
				numMapTasks = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--uniPerTask")) {
				uniPerTask = Integer.valueOf(args[++i]);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		parseArgs(args);

		Job job = new Job(getConf());
		job.setJobName("Create LUBM dataset");
		job.getConfiguration().setInt("uniPerTask", uniPerTask);
		job.setJarByClass(CreateLUBMDataset.class);

		// Input
		job.setInputFormatClass(CreateLUBMFakeReader.class);
		job.setMapperClass(CreateLUBMDatasetMapper.class);
		job.getConfiguration().setInt("maptasks", numMapTasks);

		// Mapper
		job.setMapOutputKeyClass(Triple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(TriplesPartitioner.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setReducerClass(CreateLUBMDatasetReducer.class);

		// Output of the job
		job.setOutputKeyClass(TripleSource.class);
		job.setOutputValueClass(Triple.class);
		FilesTriplesWriter.setOutputPath(job, new Path(args[0]));
		job.setOutputFormatClass(FilesTriplesWriter.class);

		// Launch
		long time = System.currentTimeMillis();
		job.waitForCompletion(true);
		log.info("Job finished in " + (System.currentTimeMillis() - time));

		return 0;
	}

}