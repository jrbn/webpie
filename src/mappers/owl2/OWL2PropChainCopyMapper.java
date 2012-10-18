package mappers.owl2;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;
import data.Triple;

public class OWL2PropChainCopyMapper extends
		Mapper<BytesWritable, BytesWritable, Triple, NullWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2PropChainCopyMapper.class);

	Triple oValue = new Triple();

	public void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		if (key.getLength() == 0) {
			oValue.setSubject(NumberUtils.decodeLong(value.getBytes(), 0));
			oValue.setPredicate(NumberUtils.decodeLong(value.getBytes(), 8));
			oValue.setObject(NumberUtils.decodeLong(value.getBytes(), 16));
			context.write(oValue, NullWritable.get());
		} else {
			int distance = NumberUtils.decodeInt(key.getBytes(), 0);
			int chainLength = NumberUtils.decodeInt(key.getBytes(), 8);
			if (distance == chainLength) {
				oValue.setSubject(NumberUtils.decodeLong(value.getBytes(), 0));
				oValue.setPredicate(NumberUtils.decodeLong(value.getBytes(), 8));
				oValue.setObject(NumberUtils.decodeLong(value.getBytes(), 16));
				context.write(oValue, NullWritable.get());
			}
		}
	}

	protected void setup(Context context) throws IOException {
	}
}
