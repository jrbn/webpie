package reducers.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ExportTriplesSampleReducer extends
		Reducer<LongWritable, NullWritable, LongWritable, BytesWritable> {

	private long threshold = 0;
	private BytesWritable oValue = new BytesWritable();

	@Override
	public void reduce(LongWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		// Count the values
		long count = 0;
		Iterator<NullWritable> itr = values.iterator();
		while (itr.hasNext()) {
			itr.next();
			++count;
		}

		if (count > threshold) {
			context.write(key, oValue);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		threshold = context.getConfiguration().getInt("reasoner.threshold", 0);
	}
}
