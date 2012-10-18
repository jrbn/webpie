package mappers.owl2;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class OWL2MergeListsMapper extends
		Mapper<BytesWritable, BytesWritable, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2MergeListsMapper.class);
	protected LongWritable oKey = new LongWritable();
	protected BytesWritable oValue = new BytesWritable();

	public void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		long start = NumberUtils.decodeLong(key.getBytes(), 0);
		long end = NumberUtils.decodeLong(key.getBytes(), 8);

		oKey.set(start);
		oValue.setSize(value.getLength() + 9);
		oValue.getBytes()[0] = 0;
		NumberUtils.encodeLong(oValue.getBytes(), 1, end);
		System.arraycopy(value.getBytes(), 0, oValue.getBytes(), 9,
				value.getLength());
		context.write(oKey, oValue);

		oKey.set(end);
		oValue.getBytes()[0] = 1;
		NumberUtils.encodeLong(oValue.getBytes(), 1, start);
		System.arraycopy(value.getBytes(), 0, oValue.getBytes(), 9,
				value.getLength());
		context.write(oKey, oValue);
	}

	protected void setup(Context context) throws IOException {
	}
}
