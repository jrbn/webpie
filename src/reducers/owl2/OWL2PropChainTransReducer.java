package reducers.owl2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class OWL2PropChainTransReducer extends
		Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2PropChainTransReducer.class);

	protected BytesWritable oKey = new BytesWritable();
	protected BytesWritable oValue = new BytesWritable();

	Set<byte[]> subjects = new HashSet<byte[]>();
	Set<byte[]> objects = new HashSet<byte[]>();

	@Override
	public void reduce(BytesWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		subjects.clear();
		objects.clear();

		if (key.getLength() == 12 || key.getLength() == 0) {
			for (BytesWritable value : values) {
				context.write(key, value);
			}
		} else {
			for (BytesWritable value : values) {
				byte[] bValue = new byte[value.getLength() - 1];
				System.arraycopy(value.getBytes(), 1, bValue, 0, value
						.getLength() - 1);
				if (value.getBytes()[0] == 0) {
					subjects.add(bValue);
				} else {
					objects.add(bValue);
				}
			}

			/* Perform the join and update the structures */
			// Copy predicate
			System.arraycopy(key.getBytes(), 0, oValue.getBytes(), 8, 8);
			for (byte[] subject : subjects) {
				for (byte[] object : objects) {
					System.arraycopy(subject, 0, oValue.getBytes(), 0, 8);
					System.arraycopy(object, 0, oValue.getBytes(), 16, 8);
					System.arraycopy(subject, 12, oKey.getBytes(), 4, 8); // Position
																			// and
																			// chain
																			// length
					int distance = NumberUtils.decodeInt(subject, 8)
							+ NumberUtils.decodeInt(object, 8);
					NumberUtils.encodeInt(oKey.getBytes(), 0, distance);
					context.write(oKey, oValue);
					context.getCounter("reasoner", "merge").increment(1);
				}
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		oKey.setSize(12);
		oValue.setSize(24);
	}
}
