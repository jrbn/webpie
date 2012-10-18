package mappers.owl2;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class OWL2FilterDuplicateListsMapper extends
		Mapper<BytesWritable, BytesWritable, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2FilterDuplicateListsMapper.class);
	protected LongWritable oKey = new LongWritable();
	protected BytesWritable oValue = new BytesWritable();

	public void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		long start = NumberUtils.decodeLong(key.getBytes(), 0);
		long end = NumberUtils.decodeLong(key.getBytes(), 8);

		oKey.set(start);
		oValue.setSize(value.getLength() + 8);
		System.arraycopy(value.getBytes(), 0, oValue.getBytes(), 8, value.getLength());
		NumberUtils.encodeLong(oValue.getBytes(), 0, end);
		context.write(oKey, oValue);
	}

	protected void setup(Context context) throws IOException { }
}
