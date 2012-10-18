package mappers.io;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.TriplesUtils;

public class SampleNTriplesMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {

	private int sample = 0;
	private Random random = new Random();

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (random.nextInt(100) < sample) {
			try {
				String[] tokens = TriplesUtils.parseTriple(value.toString(),
						"", false);
				value.set(tokens[0] + " " + tokens[1] + " " + tokens[2]);
				context.write(NullWritable.get(), value);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		sample = context.getConfiguration().getInt("sample", 0);
	}

}