package reducers.io;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamplePartitionDistrReducer extends
		Reducer<BytesWritable, NullWritable, BytesWritable, NullWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(SamplePartitionDistrReducer.class);
	long targetSize = 0;
	long currentSize = 0;
	int nPartitions = 0;
	int totalPartitions = 0;

	protected void reduce(BytesWritable key, Iterable<NullWritable> values,
			Context context) throws InterruptedException, IOException {
		++currentSize;
		if (currentSize > targetSize && (nPartitions < totalPartitions - 1)) {
			nPartitions++;
			currentSize = 0;
			context.write(key, NullWritable.get());
		}
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		nPartitions = 0;
		currentSize = 0;
		long outputMap = context.getConfiguration().getLong(
				"estimatedSampleSize", 0);
		totalPartitions = context.getConfiguration().getInt("nPartitions", 0);
		targetSize = outputMap / totalPartitions;
		log.debug("Target size: " + targetSize);
	}

	protected void cleanup(Context context) {
	}
}