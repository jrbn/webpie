package reducers.owl2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.Triple;
import data.TripleSource;

public class OWL2PropChainCopyReducer extends
		Reducer<Triple, NullWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2PropChainCopyReducer.class);
	TripleSource oKey = new TripleSource();

	@Override
	public void reduce(Triple key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		int i = 0;
		Iterator<NullWritable> itr = values.iterator();
		while (itr.hasNext()) {
			itr.next();
			i++;
		}
		if (i == 1) {
			context.write(oKey, key);
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		//FIXME: oKey.setDerivation(TripleSource.OWL2_RULE_PROPAXIOM);
		oKey.setStep(context.getConfiguration().getInt(
				"reasoner.derivationStep", 0));
	}
}
