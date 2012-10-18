package mappers.io;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import utils.NumberUtils;

public class ImportTriplesReconstructMapper extends
		Mapper<LongWritable, BytesWritable, LongWritable, LongWritable> {

	private LongWritable oKey = new LongWritable();
	private LongWritable oValue = new LongWritable();

	protected void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		long newValue = 0;
		long newKey = 0;

		long iValue = NumberUtils.decodeLong(value.getBytes(), 0);
		newValue = key.get() << 2;
		newValue |= iValue & 0x3;
		newKey = iValue >> 2;
		oKey.set(newKey);
		oValue.set(newValue);

		context.write(oKey, oValue);

	}
}