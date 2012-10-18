package reducers.owl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWLSameAsReducer extends
		Reducer<LongWritable, LongWritable, TripleSource, Triple> {

	private TripleSource oKey = new TripleSource();
	private Triple oValue = new Triple();

	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		/* init */
		Set<Long> storage = new HashSet<Long>();
		oValue.setSubject(key.get());
		boolean foundReplacement = false;

		/* Start to iterate over the values */
		for (LongWritable value : values) {
			if (value.get() < oValue.getSubject()) {
				if (foundReplacement)
					storage.add(oValue.getSubject());
				foundReplacement = true;
				oValue.setSubject(value.get());
			} else if (value.get() > oValue.getSubject()) {
				storage.add(value.get());
			}
		}

		// Empty the in-memory data structure
		for (Long value : storage) {
			oValue.setObject(value.longValue());
			context.write(oKey, oValue);
		}

		if (foundReplacement) {
			context.getCounter("synonyms", "replacements").increment(
					storage.size());
		}
	}

	@Override
	public void setup(Context context) {
		oValue.setObjectLiteral(false);
		oValue.setPredicate(TriplesUtils.OWL_SAME_AS);
		oKey.setDerivation(TripleSource.OWL_RULE_7);
		oKey.setStep(context.getConfiguration().getInt("reasoner.step", 0));
	}
}
