package mappers.owl;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWLSampleResourcesMapper extends
		Mapper<TripleSource, Triple, LongWritable, LongWritable> {

	private LongWritable oKey = new LongWritable();
	private LongWritable oValue = new LongWritable();
	private Random random = new Random();
	private int threshold = 0;

	@Override
	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		if (value.getPredicate() == TriplesUtils.OWL_SAME_AS) {
			if (value.getSubject() != value.getObject()) {
				oKey.set(value.getObject());
				oValue.set(value.getSubject());
				context.write(oKey, oValue);
			}
		} else {
			// Output only 10% of the dataset
			int randomNumber = random.nextInt(100);
			if (randomNumber < threshold) {
				oValue.set(0);
				oKey.set(value.getSubject());
				context.write(oKey, oValue);
				oKey.set(value.getPredicate());
				context.write(oKey, oValue);
				oKey.set(value.getObject());
				context.write(oKey, oValue);
			}
		}
	}

	public void setup(Context context) {
		threshold = context.getConfiguration().getInt(
				"reasoner.samplingPercentage", 0);
	}
}