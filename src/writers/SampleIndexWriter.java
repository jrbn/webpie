package writers;

import java.io.IOException;
import java.util.Arrays;

import jobs.CreateIndex;

import org.apache.hadoop.conf.Configuration;
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

public class SampleIndexWriter extends
	SequenceFileOutputFormat<BytesWritable, NullWritable> {

    @Override
    public RecordWriter<BytesWritable, NullWritable> getRecordWriter(
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
	Path file = getDefaultWorkFile(context, "");
	FileSystem fs = file.getFileSystem(conf);

	final SequenceFile.Writer[] map = new SequenceFile.Writer[CreateIndex.indices.length];
	for (int i = 0; i < map.length; ++i) {
	    String f = CreateIndex.indices[i];
	    Path p = new Path(file.getParent(), f + "/_partitions/"
		    + file.getName());
	    map[i] = SequenceFile.createWriter(fs, conf, p,
		    context.getOutputKeyClass(), context.getOutputValueClass(),
		    compressionType, codec, context);
	}

	return new RecordWriter<BytesWritable, NullWritable>() {

	    @Override
	    public void write(BytesWritable key, NullWritable value)
		    throws IOException {
		map[key.getBytes()[0]].append(
			new BytesWritable(Arrays.copyOfRange(key.getBytes(), 1,
				25)), value);
	    }

	    @Override
	    public void close(TaskAttemptContext context) throws IOException {
		for (SequenceFile.Writer writer : map) {
		    writer.close();
		}
	    }
	};
    }
}