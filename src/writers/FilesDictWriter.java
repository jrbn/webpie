package writers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class FilesDictWriter extends
		SequenceFileOutputFormat<LongWritable, BytesWritable> {

	@Override
	public RecordWriter<LongWritable, BytesWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();

		CompressionCodec codec = null;
		CompressionType compressionType = CompressionType.NONE;
		if (getCompressOutput(context)) {
			// find the kind of compression to do
			compressionType = getOutputCompressionType(context);

			// find the right codec
			Class<?> codecClass = getOutputCompressorClass(context,
					DefaultCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
		}
		// get the path of the temporary output file
		FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
		Path fileNew = new Path(committer.getWorkPath(), "new/"
				+ getUniqueFile(context, "table", ""));
		Path fileOld = new Path(committer.getWorkPath(), "old/"
				+ getUniqueFile(context, "table", ""));
		FileSystem fs = fileOld.getFileSystem(conf);
		final SequenceFile.Writer outOld = SequenceFile.createWriter(fs, conf,
				fileOld, context.getOutputKeyClass(),
				context.getOutputValueClass(), compressionType, codec, context);
		final SequenceFile.Writer outNew = SequenceFile.createWriter(fs, conf,
				fileNew, context.getOutputKeyClass(),
				context.getOutputValueClass(), compressionType, codec, context);

		return new RecordWriter<LongWritable, BytesWritable>() {

			public void write(LongWritable key, BytesWritable value)
					throws IOException {

				int length = value.getLength();
				byte flag = value.getBytes()[length - 1];
				value.setSize(length - 1);
				if (flag == 0)
					outOld.append(key, value);
				else
					outNew.append(key, value);
			}

			public void close(TaskAttemptContext context) throws IOException {
				outOld.close();
				outNew.close();
			}
		};
	}

	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
			throws IOException {
		FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
		return new Path(committer.getWorkPath(), getUniqueFile(context, "part",
				extension));
	}

}
