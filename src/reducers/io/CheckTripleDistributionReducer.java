package reducers.io;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CheckTripleDistributionReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, LongWritable> {

	private LongWritable oValue = new LongWritable();

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context) throws InterruptedException, IOException {

		oValue.set(0);
		for (IntWritable value : values) {
			oValue.set(oValue.get() + value.get());
		}

		context.write(key, oValue);
	}

	@Override
	public void setup(Context context) {
	}
}
