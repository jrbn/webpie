package mappers.io;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractTextDictionaryMapper extends
	Mapper<LongWritable, BytesWritable, LongWritable, Text> {

    protected static Logger log = LoggerFactory
	    .getLogger(ExtractTextDictionaryMapper.class);
    long number = 0;

    @Override
    protected void map(LongWritable key, BytesWritable value, Context context)
	    throws IOException, InterruptedException {
	String sValue = new String(value.getBytes(), 0, value.getLength());
	if (key.get() == number) {
	    context.write(key, new Text(sValue));
	    log.info("number = " + key + " text = " + sValue);
	}
    }

    @Override
    protected void setup(Context context) throws IOException,
	    InterruptedException {
	super.setup(context);

	number = Long.valueOf(context.getConfiguration().get("number"));
	log.info("number = " + number);
    }
}