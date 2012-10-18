package reducers.owl2;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class OWL2HasKey1Reducer extends
		Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2HasKey1Reducer.class);

	protected BytesWritable oKey = new BytesWritable();

	@Override
	public void reduce(BytesWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		boolean matchClass = false;
		boolean sizeSet = false;
		int positions = 0;
		for (BytesWritable value : values) {
			if (value.getBytes()[0] == 1) {
				matchClass = true;
			} else {
				if (!sizeSet) {
					oKey.setSize(NumberUtils.decodeInt(value.getBytes(), 13)*8);
					sizeSet = true;
				}
				int position = NumberUtils.decodeInt(value.getBytes(), 9);
				System.arraycopy(value.getBytes(), 1, oKey.getBytes(), position * 8, 8);
				positions += position + 1;
			}
		}

		//sum(n) = 1 + 2 + ... + n
		//sum(n) == n(n + 1) / 2
		if (matchClass && sizeSet && (positions == (oKey.getLength() / 8 * (oKey.getLength() / 8 + 1)) / 2)) {
			context.write(oKey, key);
		}
	}

	@Override
	public void setup(Context context) throws IOException {
	}
}
