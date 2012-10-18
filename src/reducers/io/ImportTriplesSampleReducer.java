package reducers.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.TriplesUtils;

public class ImportTriplesSampleReducer extends
		Reducer<Text, LongWritable, LongWritable, BytesWritable> {

	private long threshold = 0;
	private long counter = 0;
	private int taskId = 0;

	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();

	private Map<String, Long> preloadedURIs = null;
	private boolean noDictionary = true;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// Save the current counter
		//context.getCounter("counters", "counter-" + taskId).increment((counter & 8191));
	}

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long count = 0;
		long existingID = -1;
		Iterator<LongWritable> itr = values.iterator();

		if (!noDictionary) {
			while (itr.hasNext()) {
				LongWritable value = itr.next();
				if (value.get() > -1)
					existingID = value.get();
				else
					count++;
			}
		} else {
			while (itr.hasNext() && count <= threshold) {
				itr.next();
				count++;
			}
		}

		if (count > threshold && !preloadedURIs.containsKey(key.toString())) {
			byte[] text = key.toString().getBytes();
			oValue.setSize(text.length + 1);
			System.arraycopy(text, 0, oValue.getBytes(), 0, text.length);

			if (existingID > -1) { // Old
				oKey.set(existingID);
				oValue.getBytes()[text.length] = 0;
			} else { // New
				oKey.set(counter++);
				oValue.getBytes()[text.length] = 1;
			}
			context.write(oKey, oValue);
			/*context.getCounter("output", "records").increment(1);
			context.getCounter("output", "dictionarySize").increment(
					text.length + 8);*/
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		threshold = context.getConfiguration().getInt("reasoner.threshold", 0);
		// Set counter
		String sTaskId = context.getConfiguration().get("mapred.task.id")
				.substring(
						context.getConfiguration().get("mapred.task.id")
								.indexOf("_r_") + 3);
		taskId = Integer.valueOf(sTaskId.substring(0, sTaskId.indexOf('_')));
		noDictionary = context.getConfiguration().getBoolean(
				"ImportTriples.noDictionary", true);

		// Avoid to save the preloaded URIs because anyway they will be
		// processed as popular
		preloadedURIs = TriplesUtils.getInstance().getPreloadedURIs();

		// check whether it is already assigned.
		counter = taskId << 13;
		counter += context.getConfiguration().getInt(
				"counter-" + Integer.valueOf(taskId), 0);
		if (counter == 0) {
			counter += 100;
		}
	}
}
