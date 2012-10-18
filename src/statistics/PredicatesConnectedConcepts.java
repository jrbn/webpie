package statistics;

import java.io.IOException;
import java.net.URI;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import readers.MultiFilesReader;
import readers.NTriplesReader;
import utils.TriplesUtils;

public class PredicatesConnectedConcepts {

	private int numReduceTasks = 1;

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		if (args.length < 2) {
			System.out
					.println("Usage: CountNamespaces [input dir] [output dir]");
			System.exit(0);
		}

		new PredicatesConnectedConcepts().run(args);
	}

	public void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}
		}
	}

	public static class CountMapper extends
			Mapper<Text, Text, Text, LongWritable> {

		LongWritable output = new LongWritable(1);
		Text oKey = new Text();

		TreeSet<String> set = new TreeSet<String>();

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			try {
				String[] terms = TriplesUtils.parseTriple(value.toString(),
						key.toString(), false);

				set.clear();

				String term = terms[0];
				// For each term extract the namespace
				if (term.startsWith("<")) { // URI
					URI url = new URI(term.substring(1, term.length() - 1));
					String host = url.getHost();
					if (host.equalsIgnoreCase("linkedlifedata.com")) {
						host += url.getPath().substring(0,
								url.getPath().lastIndexOf('/'));
					}
					if (!host.equalsIgnoreCase("www.w3.org"))
						set.add(host);
				}

				term = terms[2];
				// For each term extract the namespace
				if (term.startsWith("<")) { // URI
					URI url = new URI(term.substring(1, term.length() - 1));
					String host = url.getHost();
					if (host.equalsIgnoreCase("linkedlifedata.com")) {
						host += url.getPath().substring(0,
								url.getPath().lastIndexOf('/'));
					}
					if (!host.equalsIgnoreCase("www.w3.org"))
						set.add(host);
				}

				if (set.size() > 1) {
					context.getCounter("Triples", "2 sets").increment(1);
					oKey.set(terms[1]);
					context.write(oKey, output);

					/*
					 * String term1 = set.pollFirst(); String term2 =
					 * set.pollFirst(); oKey.set(term1 + " - " + term2);
					 * context.write(oKey, output);
					 *
					 * if (set.size() == 1) { context.getCounter("Triples",
					 * "3 sets").increment(1); String term3 = set.pollFirst();
					 * oKey.set(term1 + " - " + term3); context.write(oKey,
					 * output); oKey.set(term2 + " - " + term3);
					 * context.write(oKey, output); }
					 */
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public static class CountReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable el : values) {
				count += el.get();
			}
			context.write(key, new LongWritable(count));
		}

	}

	private void run(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		parseArgs(args);

		Job job = new Job();
		job.setJobName("Counting connections namespaces");
		job.setJarByClass(PredicatesConnectedConcepts.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(NTriplesReader.class);
		MultiFilesReader.setSplitable(job.getConfiguration(), false);
		FileInputFormat.setInputPathFilter(job, OutputLogFilter.class);

		job.setMapperClass(CountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setReducerClass(CountReducer.class);
		job.setCombinerClass(CountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// TextOutputFormat.setCompressOutput(job, true);
		// TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
