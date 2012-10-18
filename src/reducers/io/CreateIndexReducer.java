package reducers.io;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CreateIndexReducer extends
		Reducer<BytesWritable, NullWritable, BytesWritable, NullWritable> {

	protected void reduce(BytesWritable key, Iterable<NullWritable> values,
			Context context) throws InterruptedException, IOException {
		context.write(key, NullWritable.get());
	}
}
