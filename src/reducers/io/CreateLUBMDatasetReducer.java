package reducers.io;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import data.Triple;
import data.TripleSource;

public class CreateLUBMDatasetReducer extends
		Reducer<Triple, NullWritable, TripleSource, Triple> {

	private TripleSource source = new TripleSource();

	@Override
	public void reduce(Triple key, Iterable<NullWritable> values,
			Context context) throws InterruptedException, IOException {
		context.write(source, key);
	}

	@Override
	public void setup(Context context) {
		source.setStep(0);
		source.setDerivation((byte) 0);
	}
}
