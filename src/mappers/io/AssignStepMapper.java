package mappers.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import data.Triple;
import data.TripleSource;

public class AssignStepMapper extends
		Mapper<TripleSource, Triple, TripleSource, Triple> {
	private int step = 0;

	protected void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		key.setStep(step);
		context.write(key, value);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		step = context.getConfiguration().getInt("step", 0);
	}

}