package mappers.io;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.mapreduce.Mapper;

import data.Triple;
import data.TripleSource;

public class SplitTriplesMapper extends
		Mapper<TripleSource, Triple, TripleSource, Triple> {

	private int split = 0;
	private Random random = new Random();

	protected void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		int nSplit = random.nextInt(split);
		key.setStep(nSplit);
		context.write(key, value);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		split = context.getConfiguration().getInt("split", 0);
	}

}