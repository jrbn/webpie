package reducers.io;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateIndexReducer extends
	Reducer<BytesWritable, NullWritable, BytesWritable, NullWritable> {

    protected static Logger log = LoggerFactory
	    .getLogger(CreateIndexReducer.class);

    @Override
    protected void reduce(BytesWritable key, Iterable<NullWritable> values,
	    Context context) throws InterruptedException, IOException {
	// Count the duplicates
//	long count = 0;
//	for (NullWritable value : values) {
//	    count++;
//	}
//
//	if (count > 2) {
//
//	    if (count <= 10)
//		context.getCounter("debug", "triples with count <= 10")
//			.increment(1);
//
//	    if (count > 10)
//		context.getCounter("debug", "triples with count > 10")
//			.increment(1);
//
//	    if (count > 100)
//		context.getCounter("debug", "triples with count > 100")
//			.increment(1);
//
//	    log.warn("Triple " + NumberUtils.decodeLong(key.getBytes(), 1)
//		    + " " + NumberUtils.decodeLong(key.getBytes(), 9) + " "
//		    + NumberUtils.decodeLong(key.getBytes(), 17)
//		    + " has count " + count);
//	}

	context.write(key, NullWritable.get());
    }
}