package mappers.io;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.NumberUtils;
import utils.TriplesUtils;

public class ImportTriplesSampleMapper extends
		Mapper<BytesWritable, BytesWritable, Text, LongWritable> {

	private Random random = new Random();
	private int sampling = 0;
	private boolean rewriteNodes;

	private Text oKey = new Text();
	private LongWritable oValue = new LongWritable();

	protected void map(BytesWritable key, BytesWritable value, Context context) {
		try {

			if (key.getBytes()[0] == 0) {
				String sKey = new String(key.getBytes(), 1, key.getLength() - 1);
				String sValue = new String(value.getBytes(), 0,
						value.getLength());
				String[] uris = TriplesUtils.parseTriple(sValue, sKey,
						rewriteNodes);
				for (String uri : uris) {
					int randomNumber = random.nextInt(100);
					if (randomNumber < sampling) {
						oKey.set(uri);
						oValue.set(-1);
						context.write(oKey, oValue);
					}
				}
			} else {
				oKey.set(new String(value.getBytes(), 0, value.getLength()));
				oValue.set(NumberUtils.decodeLong(key.getBytes(), 1));
				context.write(oKey, oValue);
			}
		} catch (Exception e) {
		}
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		sampling = context.getConfiguration().getInt(
				"reasoner.samplingPercentage", 0);
		rewriteNodes = context.getConfiguration().getBoolean(
				"ImportTriples.rewriteBlankNodes", true);
	}
}