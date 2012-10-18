package mappers.owl2;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OWL2IdentityMapper extends
		Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2IdentityMapper.class);

	public void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}

	protected void setup(Context context) throws IOException {
	}
}
