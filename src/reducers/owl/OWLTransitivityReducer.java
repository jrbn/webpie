package reducers.owl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;
import data.Triple;
import data.TripleSource;

public class OWLTransitivityReducer extends
		Reducer<BytesWritable, BytesWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLTransitivityReducer.class);

	private TripleSource source = new TripleSource();
	private Triple triple = new Triple();
	private HashMap<Long, Long> firstSet = new HashMap<Long, Long>();
	private HashMap<Long, Long> secondSet = new HashMap<Long, Long>();

	private int baseLevel = 0;
	private int level = 0;

	public void reduce(BytesWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {

		firstSet.clear();
		secondSet.clear();

		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			byte[] value = itr.next().getBytes();
			long level = NumberUtils.decodeLong(value, 1);
			long resource = NumberUtils.decodeLong(value, 9);
			if (value[0] == 0 || value[0] == 1) {
				if (!firstSet.containsKey(resource)
						|| (firstSet.containsKey(resource) && firstSet
								.get(resource) < level)) {
					if (value[0] == 1) {
						firstSet.put(resource, level * -1);
					} else {
						firstSet.put(resource, level);
					}
				}
			} else {
				if (!secondSet.containsKey(resource)
						|| (secondSet.containsKey(resource) && secondSet
								.get(resource) < level)) {
					if (value[0] == 3) {
						secondSet.put(resource, level * -1);
					} else {
						secondSet.put(resource, level);
					}
				}
			}
		}

		if (firstSet.size() == 0 || secondSet.size() == 0)
			return;

		triple.setPredicate(NumberUtils.decodeLong(key.getBytes(), 0));
		Iterator<Entry<Long, Long>> firstItr = firstSet.entrySet().iterator();
		context.getCounter("stats", "joinPoint").increment(1);
		while (firstItr.hasNext()) {
			Entry<Long, Long> entry = firstItr.next();
			Iterator<Entry<Long, Long>> secondItr = secondSet.entrySet()
					.iterator();
			while (secondItr.hasNext()) {
				Entry<Long, Long> entry2 = secondItr.next();
				// Output the triple
				if (level != 1
						|| (level == 1 && (entry.getValue() > 0 || entry2
								.getValue() > 0))) {
					triple.setSubject(entry.getKey());
					triple.setObject(entry2.getKey());
					source.setStep((int) (Math.abs(entry.getValue())
							+ Math.abs(entry2.getValue()) - baseLevel));
					context.write(source, triple);
				}
			}
		}
	}

	@Override
	public void setup(Context context) {
		baseLevel = context.getConfiguration().getInt("reasoning.baseLevel", 1) - 1;
		level = context.getConfiguration().getInt(
				"reasoning.transitivityLevel", -1);
		source.setDerivation(TripleSource.OWL_RULE_4);
		triple.setObjectLiteral(false);
	}
}
