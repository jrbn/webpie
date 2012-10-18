package mappers.io;


import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import readers.FilesDictReader;

public class ExportTriplesJoinDictMapper extends
		Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	private Set<Long> commonResources = null;

	protected void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		if (commonResources.contains(key.get())) {
			context.write(key, value);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// Load common resources
		try {
			String path = context.getConfiguration().get("commonResources");
			commonResources = FilesDictReader.readSetCommonResources(context
					.getConfiguration(), new Path(path));
		} catch (Exception e) {
		}
	}
}