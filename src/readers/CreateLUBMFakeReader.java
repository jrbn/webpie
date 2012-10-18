package readers;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CreateLUBMFakeReader extends FileInputFormat<LongWritable, Text> {

	public class FakeReader extends RecordReader<LongWritable, Text> {

		boolean firstTime = false;

		@Override
		public void close() throws IOException {
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return new LongWritable();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return new Text();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			firstTime = false;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (firstTime)
				return false;
			else {
				firstTime = true;
				return true;
			}
		}

	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new FakeReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		int numSplits = job.getConfiguration().getInt("maptasks", 1);
		List<InputSplit> splits = new LinkedList<InputSplit>();

		for (int i = 0; i < numSplits; ++i) {
			splits.add(new FileSplit(new Path("data"), 0, 0, null));
		}

		return splits;
	}

}
