package writers;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class FilesCombinedWriter extends
		OutputFormat<LongWritable, BytesWritable> {

	SequenceFileOutputFormat<LongWritable, BytesWritable> statsFormat = new SequenceFileOutputFormat<LongWritable, BytesWritable>();
	FilesDictWriter dictFormat = new FilesDictWriter();

	public class FilesCombinedRecordWriter extends
			RecordWriter<LongWritable, BytesWritable> {

		private RecordWriter<LongWritable, BytesWritable> dictionaryStream = null;
		private RecordWriter<LongWritable, BytesWritable> statementsStream = null;

		public FilesCombinedRecordWriter(
				RecordWriter<LongWritable, BytesWritable> dictionary,
				RecordWriter<LongWritable, BytesWritable> statements) {
			dictionaryStream = dictionary;
			statementsStream = statements;
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			dictionaryStream.close(context);
			statementsStream.close(context);
		}

		@Override
		public void write(LongWritable key, BytesWritable value)
				throws IOException, InterruptedException {
			// Check whether it is a dictionary entry or a statement entry
			if (value.getBytes()[value.getLength() - 1] != 0) {
				value.setSize(value.getLength() - 1);
				dictionaryStream.write(key, value);
			} else {
				value.setSize(value.getLength() - 1);
				statementsStream.write(key, value);
			}
		}
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		dictFormat.checkOutputSpecs(context);
		statsFormat.checkOutputSpecs(context);
	}

	@Override
	public RecordWriter<LongWritable, BytesWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new FilesCombinedRecordWriter(
				dictFormat.getRecordWriter(context),
				statsFormat.getRecordWriter(context));
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return statsFormat.getOutputCommitter(context);
	}

}
