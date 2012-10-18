package reducers.owl2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWL2HasKey2Reducer extends
		Reducer<BytesWritable, BytesWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2HasKey2Reducer.class);

	protected TripleSource oKey = new TripleSource();
	protected Triple oValue = new Triple();

	Map<Long, ArrayList<Long>> map = new HashMap<Long, ArrayList<Long>>();

	@Override
	public void reduce(BytesWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		map.clear();

		for (BytesWritable value : values) {
			long chain = NumberUtils.decodeLong(value.getBytes(), 8);
			if (!map.containsKey(chain)) {
				map.put(chain, new ArrayList<Long>());
			}

			Collection<Long> col = map.get(chain);
			col.add(NumberUtils.decodeLong(value.getBytes(), 0));
		}

		for (ArrayList<Long> entry : map.values()) {
			long smallestEntity = Long.MAX_VALUE;
			for (long entity : entry) {
				if (entity < smallestEntity) {
					smallestEntity = entity;
				}
			}

			oValue.setSubject(smallestEntity);
			for (long entity : entry) {
				if (entity > smallestEntity) {
					oValue.setObject(entity);
					context.write(oKey, oValue);
				}
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		oKey.setStep(context.getConfiguration().getInt("reasoner.step", -1));
		oKey.setDerivation(TripleSource.OWL2_RULE_HASKEY);
		oValue.setPredicate(TriplesUtils.OWL_SAME_AS);
	}
}
