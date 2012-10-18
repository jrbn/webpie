package reducers.owl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;

import utils.NumberUtils;
import data.Triple;
import data.TripleSource;

public class OWLNotRecursiveReducer extends
		Reducer<BytesWritable, LongWritable, TripleSource, Triple> {

	private static Logger log = LoggerFactory
			.getLogger(OWLNotRecursiveReducer.class);

	private Triple triple = new Triple();
	private TripleSource source = new TripleSource();
	// private TripleSource transitiveSource = new TripleSource();
	private Set<Long> set = new HashSet<Long>();

	protected Map<Long, Collection<Long>> schemaInverseOfProperties = null;

	protected void reduce(BytesWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		byte[] bytes = key.getBytes();

		switch (bytes[0]) {
		case 0: // Functional and inverse functional property
			long minimum = Long.MAX_VALUE;
			set.clear();
			Iterator<LongWritable> itr = values.iterator();
			while (itr.hasNext()) {
				long value = itr.next().get();
				if (value < minimum) {
					if (minimum != Long.MAX_VALUE)
						set.add(minimum);
					minimum = value;
				} else {
					set.add(value);
				}
			}
			triple.setObjectLiteral(false);
			triple.setSubject(minimum);
			triple.setPredicate(utils.TriplesUtils.OWL_SAME_AS);
			Iterator<Long> itr2 = set.iterator();
			long outputSize = 0;
			while (itr2.hasNext()) {
				long object = itr2.next();
				triple.setObject(object);
				context.write(source, triple);
				outputSize++;
			}
			context.getCounter("OWL derived triples",
					"functional and inverse functional property").increment(
					outputSize);

			break;
		case 2: // Symmetric property
			long subject = NumberUtils.decodeLong(bytes, 1);
			long object = NumberUtils.decodeLong(bytes, 9);
			triple.setSubject(object);
			triple.setObject(subject);
			triple.setObjectLiteral(false);
			itr = values.iterator();
			while (itr.hasNext()) {
				triple.setPredicate(itr.next().get());
				context.write(source, triple);
				context.getCounter("OWL derived triples", "simmetric property")
						.increment(1);
			}
			break;
		case 3: // Inverse of property
			subject = NumberUtils.decodeLong(bytes, 1);
			object = NumberUtils.decodeLong(bytes, 9);
			triple.setObjectLiteral(false);
			set.clear();
			itr = values.iterator();
			while (itr.hasNext()) {
				triple.setObject(subject);
				triple.setSubject(object);
				long predicate = itr.next().get();
				/* I only output the last key of the inverse */
				Collection<Long> inverse = schemaInverseOfProperties
						.get(predicate);
				if (inverse != null) {
					Iterator<Long> itrInverse = inverse.iterator();
					triple.setPredicate(itrInverse.next());
					context.write(source, triple);
					context.getCounter("OWL derived triples", "inverse of")
							.increment(1);
				} else {
					log.error("Something is wrong here. This should not happen...");
				}

				set.add(predicate);
				outputInverseOf(subject, object, predicate, set, context);
			}
			break;
		default:
			break;
		}

	}

	private void outputInverseOf(long subject, long object, long predicate,
			Set<Long> alreadyDerived, Context context) throws IOException,
			InterruptedException {
		Collection<Long> col = schemaInverseOfProperties.get(predicate);
		if (col != null) {
			Iterator<Long> itr = col.iterator();
			while (itr.hasNext()) {
				long inverseOf = itr.next();
				if (!alreadyDerived.contains(inverseOf)) {
					alreadyDerived.add(inverseOf);
					triple.setSubject(object);
					triple.setObject(subject);
					triple.setPredicate(inverseOf);
					context.write(source, triple);
					context.getCounter("OWL derived triples", "inverse of")
							.increment(1);
					outputInverseOf(object, subject, inverseOf, alreadyDerived,
							context);
				}
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		source.setDerivation(TripleSource.OWL_RULE_1);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));

		if (schemaInverseOfProperties == null) {
			schemaInverseOfProperties = FilesTriplesReader.loadMapIntoMemory(
					"FILTER_ONLY_OWL_INVERSE_OF", context);
		}
	}
}
