package mappers.io;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class CreateIndexMapper extends
		Mapper<TripleSource, Triple, BytesWritable, NullWritable> {

	BytesWritable oValue = new BytesWritable();
	String index = null;

	protected void map(TripleSource key, Triple value, Context context)
			throws InterruptedException, IOException {
		if (index != null) {
			TriplesUtils.createTripleIndex(oValue.getBytes(), value, index);
			context.write(oValue, NullWritable.get());
		}
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		oValue.setSize(24);
		index = context.getConfiguration().get("indexType");
	}
}
