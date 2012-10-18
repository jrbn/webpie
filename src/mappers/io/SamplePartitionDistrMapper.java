package mappers.io;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class SamplePartitionDistrMapper extends
		Mapper<TripleSource, Triple, BytesWritable, NullWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(SamplePartitionDistrMapper.class);

	int samplingPercentage = 0;
	Random random = new Random();
	String indexType = null;
	BytesWritable oValue = null;

	protected void map(TripleSource key, Triple value, Context context)
			throws InterruptedException, IOException {
		if (random.nextInt(100) < samplingPercentage) {
			TriplesUtils.createTripleIndex(oValue.getBytes(), value, indexType);
			context.write(oValue, NullWritable.get());
		}
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		samplingPercentage = context.getConfiguration().getInt(
				"samplingPercentage", 0);
		indexType = context.getConfiguration().get("indexType");
		oValue = new BytesWritable();
		oValue.setSize(24);
	}
}
