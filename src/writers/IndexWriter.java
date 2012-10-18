package writers;

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class IndexWriter extends
		SequenceFileOutputFormat<BytesWritable, NullWritable> {

	private class IndexRecordWriter extends
			RecordWriter<BytesWritable, NullWritable> {

		int chunk = 0;
		int count = 0;
		int limit = 0;
		TaskAttemptContext context = null;
		SequenceFile.Writer out = null;

		public IndexRecordWriter(TaskAttemptContext context) throws IOException {
			this.context = context;
			limit = context.getConfiguration().getInt("sizeChunk", 0);
			createNewFile();
		}

		private void createNewFile() throws IOException {
			if (out != null) {
				out.close();
			}

			CompressionCodec codec = null;
			CompressionType compressionType = CompressionType.NONE;
			if (getCompressOutput(context)) {
				compressionType = getOutputCompressionType(context);
				Class<?> codecClass = getOutputCompressorClass(context,
						DefaultCodec.class);
				codec = (CompressionCodec) ReflectionUtils.newInstance(
						codecClass, context.getConfiguration());
			}
			
			NumberFormat format = NumberFormat.getNumberInstance();
			format.setGroupingUsed(false);
			format.setMinimumIntegerDigits(5);
			Path file = getDefaultWorkFile(context, "_" + format.format(chunk));
			out = SequenceFile.createWriter(FileSystem.get(context
					.getConfiguration()), context.getConfiguration(), file,
					context.getOutputKeyClass(), context.getOutputValueClass(),
					compressionType, codec, context);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			if (out != null) {
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
			
			out.append(key, value);
		}

	}

	@Override
	public RecordWriter<BytesWritable, NullWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new IndexRecordWriter(context);
	}

}
