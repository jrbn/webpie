package mappers.owl2;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class OWL2PropChainTransMapper extends
		Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2PropChainTransMapper.class);
	/* private int currentExecution = -1; */
	private int pow = -1;

	protected BytesWritable oKey = new BytesWritable();
	protected BytesWritable oValue = new BytesWritable();

	protected Map<Long, Collection<byte[]>> properties = null;

	public void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		if (key.getLength() == 0) {
			context.write(key, value);
		} else {
			int distance = NumberUtils.decodeInt(key.getBytes(), 0);
			int position = NumberUtils.decodeInt(key.getBytes(), 4);
			int chainLength = NumberUtils.decodeInt(key.getBytes(), 8);
	
			if ((position == ((chainLength / pow) * pow) && position > 0)
					|| (distance == chainLength)) {
				context.write(key, value);
			} else {
				// oKey: matching point
				// oValue: other resource + distance + position + chainLength
				NumberUtils.encodeInt(oValue.getBytes(), 9, distance);
				NumberUtils.encodeInt(oValue.getBytes(), 13, position);
				NumberUtils.encodeInt(oValue.getBytes(), 17, chainLength);
				if (position % pow == 0) {
					NumberUtils.encodeLong(oKey.getBytes(), 0, NumberUtils
							.decodeLong(value.getBytes(), 8)); //Predicate
					NumberUtils.encodeLong(oKey.getBytes(), 8, NumberUtils
							.decodeLong(value.getBytes(), 16)); //Object
					NumberUtils.encodeLong(oValue.getBytes(), 1, NumberUtils
							.decodeLong(value.getBytes(), 0));
					oValue.getBytes()[0] = 0;
				} else {
					NumberUtils.encodeLong(oKey.getBytes(), 0, NumberUtils
							.decodeLong(value.getBytes(), 8));
					NumberUtils.encodeLong(oKey.getBytes(), 8, NumberUtils
							.decodeLong(value.getBytes(), 0));
					NumberUtils.encodeLong(oValue.getBytes(), 1, NumberUtils
							.decodeLong(value.getBytes(), 16));
					oValue.getBytes()[0] = 1;
				}
				context.write(oKey, oValue);
			}
		}
	}

	protected void setup(Context context) throws IOException {
		oKey.setSize(16);
		oValue.setSize(21);

		int step = context.getConfiguration().getInt("step", -1);
		pow = (int) Math.pow(2, step);
	}
}
