package mappers.io;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ExportTriplesReconstructMapper extends
		Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	protected void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
