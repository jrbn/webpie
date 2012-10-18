package reducers.owl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OWLSampleResourcesReducer extends
		Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	// private static Logger log =
	// LoggerFactory.getLogger(OWLSampleResourcesReducer.class);
	private long threshold = 0;
	private LongWritable oValue = new LongWritable();

	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		// Count the values
		long count = 0;
		oValue.set(0);
		Iterator<LongWritable> itr = values.iterator();
		while (itr.hasNext()) {
			long currentValue = itr.next().get();
			if (currentValue > 0) { // There is a substitution
				oValue.set(currentValue);
			} else {
				++count;
			}
		}

		if (count > threshold) {
			context.write(key, oValue);
		}
	}

	@Override
	public void setup(Context context) {
		threshold = context.getConfiguration().getInt("reasoner.threshold", 0);
	}
}
