package reducers.owl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;

import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWLHasValueReducer extends
		Reducer<LongWritable, BytesWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLHasValueReducer.class);

	private Triple triple = new Triple();
	private TripleSource source = new TripleSource();

	private Map<Long, Collection<Long>> hasValueMap = new HashMap<Long, Collection<Long>>();
	private Map<Long, Collection<Long>> onPropertyMap = new HashMap<Long, Collection<Long>>();

	private Map<Long, Collection<Long>> hasValue2Map = new HashMap<Long, Collection<Long>>();
	private Map<Long, Collection<Long>> onProperty2Map = new HashMap<Long, Collection<Long>>();

	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {

		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			byte[] v = itr.next().getBytes();
			if (v.length > 0) {
				if (v[0] == 0) { // Rule 14b
					long object = NumberUtils.decodeLong(v, 1);
					Collection<Long> props = onPropertyMap.get(object);
					if (props != null) {
						Collection<Long> hasValues = hasValueMap.get(object);
						if (hasValues != null) {
							triple.setSubject(key.get());
							Iterator<Long> itr2 = props.iterator();
							while (itr2.hasNext()) {
								long prop = itr2.next();
								triple.setPredicate(prop);
								Iterator<Long> itr3 = hasValues.iterator();
								while (itr3.hasNext()) {
									long value = itr3.next();
									triple.setObject(value);
									context.write(source, triple);
								}
							}
						}
					}
				} else { // Rule 14a
					long predicate = NumberUtils.decodeLong(v, 1);
					long object = NumberUtils.decodeLong(v, 9);

					Collection<Long> types = hasValue2Map.get(object);
					Collection<Long> pred = onProperty2Map.get(predicate);
					if (types != null && pred != null) {
						types.retainAll(pred);
						Iterator<Long> itr4 = types.iterator();
						triple.setSubject(key.get());
						triple.setPredicate(TriplesUtils.RDF_TYPE);
						while (itr4.hasNext()) {
							long type = itr4.next();
							triple.setObject(type);
							context.write(source, triple);
						}
					}
				}
			}
		}
	}

	public void setup(Context context) throws IOException {
		source.setDerivation(TripleSource.OWL_RULE_14A);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));
		triple.setObjectLiteral(false);

		// Load the schema triples
		hasValueMap = FilesTriplesReader.loadMapIntoMemory(
				"FILTER_ONLY_OWL_HAS_VALUE", context);
		onPropertyMap = FilesTriplesReader.loadMapIntoMemory(
				"FILTER_ONLY_OWL_ON_PROPERTY", context);

		hasValue2Map = FilesTriplesReader.loadMapIntoMemory(
				"FILTER_ONLY_OWL_HAS_VALUE", context, true);
		onProperty2Map = FilesTriplesReader.loadMapIntoMemory(
				"FILTER_ONLY_OWL_ON_PROPERTY", context, true);
	}
}
