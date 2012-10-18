package mappers.io;

import java.io.IOException;

import jobs.CreateIndex;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class CreateIndexMapper extends
	Mapper<TripleSource, Triple, BytesWritable, NullWritable> {

    protected static Logger log = LoggerFactory
	    .getLogger(CreateIndexMapper.class);

    BytesWritable oValue = new BytesWritable();

    @Override
    protected void map(TripleSource key, Triple value, Context context)
	    throws InterruptedException, IOException {
	for (int i = 0; i < 6; ++i) {
	    String index = CreateIndex.indices[i];
	    oValue.getBytes()[0] = (byte) i;
	    TriplesUtils.createTripleIndex(oValue.getBytes(), 1, value, index);
	    context.write(oValue, NullWritable.get());
	}
    }

    @Override
    protected void setup(Context context) throws IOException,
	    InterruptedException {
	oValue.setSize(25);	
    }
}
