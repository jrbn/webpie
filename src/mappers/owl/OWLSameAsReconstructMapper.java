package mappers.owl;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class OWLSameAsReconstructMapper extends Mapper<LongWritable, BytesWritable, BytesWritable, BytesWritable> {
	
	protected static Logger log = LoggerFactory.getLogger(OWLSameAsReconstructMapper.class);

	private BytesWritable oKey = new BytesWritable();
	private BytesWritable oValue = new BytesWritable();
	private byte[] bValue = new byte[9];

	public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
		oKey.set(value.getBytes(), 1, 13);	
		bValue[0] = value.getBytes()[0];
		NumberUtils.encodeLong(bValue, 1, key.get());
		context.write(oKey, oValue);
	}
	
	@Override
	public void setup(Context context) {
		oValue = new BytesWritable(bValue);
	}
}
