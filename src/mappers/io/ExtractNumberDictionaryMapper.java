package mappers.io;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractNumberDictionaryMapper extends
		Mapper<LongWritable, BytesWritable, LongWritable, Text> {

	protected static Logger log = LoggerFactory
			.getLogger(ExtractNumberDictionaryMapper.class);
	String uri = null;

	protected void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		String sValue = new String(value.getBytes(), 0, value.getLength());
		if (sValue.endsWith(uri)) {
			context.write(key, new Text(sValue));
			log.info("number = " + key + " text = " + uri);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		uri = context.getConfiguration().get("uri");
		log.info("URI = " + uri);
	}

}