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

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree.ByteResourceNode;
import data.Tree.Node.Rule;
import data.Triple;
import data.TripleSource;

public class OWLTransitivityReducer
		extends
		Reducer<BytesWritable, ProtobufWritable<ByteResourceNode>, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLTransitivityReducer.class);

	private TripleSource source = new TripleSource();
	private Triple triple = new Triple();
	private HashMap<Long, ByteResourceNode> firstSet = new HashMap<Long, ByteResourceNode>();
	private HashMap<Long, ByteResourceNode> secondSet = new HashMap<Long, ByteResourceNode>();

	private int baseLevel = 0;
	private int level = 0;

	@Override
	public void reduce(BytesWritable key,
			Iterable<ProtobufWritable<ByteResourceNode>> values, Context context)
			throws IOException, InterruptedException {

		firstSet.clear();
		secondSet.clear();

		for (ProtobufWritable<ByteResourceNode> value : values) {
			value.setConverter(ByteResourceNode.class);
			ByteResourceNode brn = value.get();

			long level = brn.getHistory().getStep();
			long resource = brn.getResource();
			if (brn.getId() == 0 || brn.getId() == 1) {
				if (!firstSet.containsKey(resource)
						|| (firstSet.containsKey(resource) && firstSet
								.get(resource).getHistory().getStep() < level)) {
					/*
					 * if (brn.getId() == 1) { brn.getHistory().setStep(level *
					 * -1); firstSet.put(resource, level * -1); } else {
					 */
					firstSet.put(resource, brn);
					// }
				}
			} else {
				if (!secondSet.containsKey(resource)
						|| (secondSet.containsKey(resource) && secondSet
								.get(resource).getHistory().getStep() < level)) {
					// if (brn.getId() == 3) {
					// secondSet.put(resource, level * -1);
					// } else {
					secondSet.put(resource, brn);
					// }
				}
			}
		}

		if (firstSet.size() == 0 || secondSet.size() == 0)
			return;

		triple.setPredicate(NumberUtils.decodeLong(key.getBytes(), 0));
		Iterator<Entry<Long, ByteResourceNode>> firstItr = firstSet.entrySet()
				.iterator();
		context.getCounter("stats", "joinPoint").increment(1);
		while (firstItr.hasNext()) {
			Entry<Long, ByteResourceNode> entry = firstItr.next();
			Iterator<Entry<Long, ByteResourceNode>> secondItr = secondSet
					.entrySet().iterator();
			while (secondItr.hasNext()) {
				Entry<Long, ByteResourceNode> entry2 = secondItr.next();
				// Output the triple
				if (level != 1
						|| (level == 1 && (entry.getValue().getId() != 1 || entry2
								.getValue().getId() != 3))) {
					triple.setSubject(entry.getKey());
					triple.setObject(entry2.getKey());
					source.setStep((Math.abs(entry.getValue().getHistory().getStep())
							+ Math.abs(entry2.getValue().getHistory().getStep()) - baseLevel));
					source.clearChildren();
					source.addChild(entry.getValue().getHistory());
					source.addChild(entry2.getValue().getHistory());
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
		source.setRule(Rule.OWL_TRANS);
		triple.setObjectLiteral(false);
	}
}
