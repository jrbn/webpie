package mappers.io;

import java.io.IOException;
import java.util.Random;

import jobs.CreateIndex;

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
    // String indexType = null;
    BytesWritable oValue = null;

    // String[] indeces = null;

    @Override
    protected void map(TripleSource key, Triple value, Context context)
	    throws InterruptedException, IOException {
	if (random.nextInt(1000) < samplingPercentage) {
	    for (int i = 0; i < CreateIndex.indices.length; ++i) {
		String index = CreateIndex.indices[i];
		oValue.getBytes()[0] = (byte) i;
		TriplesUtils.createTripleIndex(oValue.getBytes(), 1, value,
			index);
		context.write(oValue, NullWritable.get());
	    }
	}
    }

    @Override
    protected void setup(Context context) throws IOException,
	    InterruptedException {
	samplingPercentage = context.getConfiguration().getInt(
		"samplingPercentage", 0);
	oValue = new BytesWritable();
	oValue.setSize(25);
    }
}
