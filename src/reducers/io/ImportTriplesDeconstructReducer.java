package reducers.io;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class ImportTriplesDeconstructReducer extends
		Reducer<Text, LongWritable, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(ImportTriplesDeconstructReducer.class);
	private LongWritable oKey = new LongWritable();
	private long counter = 0;
	private long taskId = 0;
	private List<byte[]> list = new LinkedList<byte[]>();
	private boolean noDictionary = true;

	private BytesWritable tripleValue = new BytesWritable();
	private BytesWritable dictValue = new BytesWritable();

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		if (key.toString().startsWith("@FAKE")) {
			String id = key.toString().substring(
					key.toString().indexOf('-') + 1);
			oKey.set(Long.valueOf(id));

			// output node id + triple id
			for (LongWritable value : values) {
				tripleValue.setSize(9);
				tripleValue.getBytes()[8] = 0;
				NumberUtils.encodeLong(tripleValue.getBytes(), 0, value.get());
				context.write(oKey, tripleValue);
			}
		} else {

			oKey.set(0);

			if (!noDictionary) {
				list.clear();
				for (LongWritable value : values) {
					if (value.get() < 0) {
						oKey.set(Math.abs(value.get()));
						// TODO: empty the list
					} else {
						byte[] valueToStore = new byte[8];
						NumberUtils.encodeLong(valueToStore, 0, value.get());
						list.add(valueToStore);
					}
				}
				if (list.size() == 0)
					return;
			}

			if (oKey.get() == 0) { // Never assigned
				oKey.set(counter++);
				context.getCounter("stats", "ass").increment(1);
				// Save dictionary
				byte[] bytes = key.toString().getBytes();
				dictValue.setSize(bytes.length + 2);
				System.arraycopy(bytes, 0, dictValue.getBytes(), 0,
						bytes.length);
				dictValue.getBytes()[bytes.length] = 1;
				dictValue.getBytes()[bytes.length + 1] = 1;
				context.write(oKey, dictValue);
				context.getCounter("output", "dictionarySize").increment(
						bytes.length + 8);
			}

			if (!noDictionary) {
				// output node id + triple id
				for (byte[] value : list) {
					tripleValue.setSize(9);
					tripleValue.getBytes()[8] = 0;
					System.arraycopy(value, 0, tripleValue.getBytes(), 0, 8);
					context.write(oKey, tripleValue);
				}
			} else {
				for (LongWritable value : values) {
					tripleValue.setSize(9);
					tripleValue.getBytes()[8] = 0;
					NumberUtils.encodeLong(tripleValue.getBytes(), 0,
							value.get());
					context.write(oKey, tripleValue);
				}
			}
		}
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		String sTaskId = context
				.getConfiguration()
				.get("mapred.task.id")
				.substring(
						context.getConfiguration().get("mapred.task.id")
								.indexOf("_r_") + 3);
		taskId = Long.valueOf(sTaskId.substring(0, sTaskId.indexOf('_')));
		noDictionary = context.getConfiguration().getBoolean(
				"ImportTriples.noDictionary", true);

		// Load the counter from the job
		counter = (taskId + 1) << 40;
		counter += context.getConfiguration().getInt("counter-" + taskId, 0);
		log.debug("Start counter " + counter + "(" + (taskId + 1) + ")");
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		context.getCounter("counter-" + taskId,
				Long.toString(counter & Integer.MAX_VALUE)).increment(1);
	}
}
