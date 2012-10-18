package writers;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexWriter2 extends FileOutputFormat<BytesWritable, NullWritable> {

	private class IndexRecordWriter extends
			RecordWriter<BytesWritable, NullWritable> {

		int chunk = 0;
		int count = 0;
		int limit = 0;
		TaskAttemptContext context = null;
		FSDataOutputStream out = null;
		GZIPOutputStream fout = null;

		public IndexRecordWriter(TaskAttemptContext context) throws IOException {
			this.context = context;
			limit = context.getConfiguration().getInt("sizeChunk", 0);
			createNewFile();
		}

		private void createNewFile() throws IOException {
			if (out != null) {
				fout.close();
				out.close();
			}

			NumberFormat format = NumberFormat.getNumberInstance();
			format.setGroupingUsed(false);
			format.setMinimumIntegerDigits(5);
			Path file = getDefaultWorkFile(context, "_" + format.format(chunk));

			out = FileSystem.get(context.getConfiguration()).create(file);
			fout = new GZIPOutputStream(out);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			if (out != null) {
				fout.close();
				out.close();
			}
		}

		@Override
		public void write(BytesWritable key, NullWritable value)
				throws IOException, InterruptedException {
			++count;
			if (count >= limit) {
				chunk++;
				createNewFile();
				count = 1;
			}

			fout.write(key.getBytes(), 0, 24);
		}

	}

	@Override
	public RecordWriter<BytesWritable, NullWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new IndexRecordWriter(context);
	}

}