package jobs;

import mappers.io.ExtractTextDictionaryMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractTextDictionary extends Configured implements Tool {

    protected static Logger log = LoggerFactory
	    .getLogger(ExtractTextDictionary.class);

    public static void main(String[] args) throws Exception {

	if (args.length < 2) {
	    System.out
		    .println("Usage: ExtractTextDictionary [dict dir] [output dir] --number <number>");
	    System.exit(0);
	}

	int res = ToolRunner.run(new Configuration(),
		new ExtractTextDictionary(), args);
	System.exit(res);
    }

    long number;

    @Override
    public int run(String[] args) throws Exception {

	for (int i = 0; i < args.length; ++i) {
	    if (args[i].equalsIgnoreCase("--number")) {
		number = Long.valueOf(args[++i]);
	    }
	}

	Job job = new Job(this.getConf());
	job.setJobName("Extract text from dictionary");
	job.setJarByClass(ExtractTextDictionary.class);

	job.setInputFormatClass(SequenceFileInputFormat.class);
	FileInputFormat.setInputPathFilter(job, OutputLogFilter.class);
	SequenceFileInputFormat.addInputPath(job, new Path(args[0], "0"));
	SequenceFileInputFormat.addInputPath(job, new Path(args[0], "1"));
	job.setMapperClass(ExtractTextDictionaryMapper.class);
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(Text.class);

	job.getConfiguration().set("number", Long.toString(number));
	job.setNumReduceTasks(0);
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(Text.class);
	TextOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setOutputFormatClass(TextOutputFormat.class);
	job.waitForCompletion(true);

	return 0;
    }
}
