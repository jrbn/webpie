package reducers.owl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import utils.NumberUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree;
import data.Tree.Node;
import data.Tree.Node.Rule;
import data.Tree.ResourceNode;
import data.Triple;
import data.TripleSource;

public class OWLNotRecursiveReducer
		extends
		Reducer<BytesWritable, ProtobufWritable<ResourceNode>, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLNotRecursiveReducer.class);

	private Triple triple = new Triple();
	private TripleSource sourceFunctional = new TripleSource();
	private TripleSource sourceInvFunctional = new TripleSource();
	private TripleSource sourceSymmetric = new TripleSource();
	private TripleSource sourceInverseOf = new TripleSource();

	private Map<Long, Tree.Node> set = new HashMap<Long, Tree.Node>();
	private Set<Long> set2 = new HashSet<Long>();

	protected Map<Long, Collection<ResourceNode>> schemaInverseOfProperties = null;

	@Override
	protected void reduce(BytesWritable key,
			Iterable<ProtobufWritable<ResourceNode>> values, Context context)
			throws IOException, InterruptedException {
		byte[] bytes = key.getBytes();

		switch (bytes[0]) {
		case 0:
		case 1: // Functional and inverse functional property
			long minimum = Long.MAX_VALUE;
			Node minHistory = null;
			set.clear();
			Iterator<ProtobufWritable<ResourceNode>> itr = values.iterator();
			while (itr.hasNext()) {
				ProtobufWritable<ResourceNode> value = itr.next();
				value.setConverter(ResourceNode.class);
				ResourceNode rn = value.get();
				long resource = rn.getResource();
				if (resource < minimum) {
					if (minimum != Long.MAX_VALUE)
						set.put(minimum, minHistory);
					minimum = resource;
					minHistory = rn.getHistory();
				} else {
					set.put(resource, rn.getHistory());
				}
			}

			triple.setObjectLiteral(false);
			triple.setSubject(minimum);
			triple.setPredicate(utils.TriplesUtils.OWL_SAME_AS);

			long outputSize = 0;
			TripleSource source = null;
			if (bytes[0] == 0) {
				source = sourceFunctional;
			} else {
				source = sourceInvFunctional;
			}
			for (Map.Entry<Long, Node> entry : set.entrySet()) {
				long object = entry.getKey();
				triple.setObject(object);
				source.clearChildren();
				source.addChild(minHistory);
				source.addChild(entry.getValue());
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
				ProtobufWritable<ResourceNode> value = itr.next();
				value.setConverter(ResourceNode.class);
				ResourceNode rn = value.get();
				triple.setPredicate(rn.getResource());
				sourceSymmetric.clearChildren();
				sourceSymmetric.addChild(rn.getHistory());
				context.write(sourceSymmetric, triple);
				context.getCounter("OWL derived triples", "simmetric property")
						.increment(1);
			}
			break;
		case 3: // Inverse of property
			subject = NumberUtils.decodeLong(bytes, 1);
			object = NumberUtils.decodeLong(bytes, 9);
			triple.setObjectLiteral(false);
			set2.clear();
			itr = values.iterator();
			while (itr.hasNext()) {
				triple.setObject(subject);
				triple.setSubject(object);

				ProtobufWritable<ResourceNode> value = itr.next();
				value.setConverter(ResourceNode.class);
				ResourceNode rn = value.get();

				long predicate = rn.getResource();
				/* I only output the last key of the inverse */
				/*
				 * Collection<ResourceNode> inverse = schemaInverseOfProperties
				 * .get(predicate); if (inverse != null) { Iterator<Long>
				 * itrInverse = inverse.iterator();
				 * triple.setPredicate(itrInverse.next()); context.write(source,
				 * triple); context.getCounter("OWL derived triples",
				 * "inverse of") .increment(1); } else {
				 * log.error("Something is wrong here. This should not happen..."
				 * ); }
				 */

				set2.add(predicate);
				outputInverseOf(rn.getHistory(), subject, object, predicate, set2, context);
			}
			break;
		default:
			break;
		}

	}

	private void outputInverseOf(Node source, long subject, long object, long predicate,
			Set<Long> alreadyDerived, Context context) throws IOException,
			InterruptedException {

		Collection<ResourceNode> col = schemaInverseOfProperties.get(predicate);

		if (col != null) {
			for (ResourceNode rn : col) {
				long inverseOf = rn.getResource();
				if (!alreadyDerived.contains(inverseOf)) {
					alreadyDerived.add(inverseOf);

					triple.setSubject(object);
					triple.setObject(subject);
					triple.setPredicate(inverseOf);

					sourceInverseOf.clearChildren();
					sourceInverseOf.addChild(source);
					sourceInverseOf.addChild(rn.getHistory());

					context.write(sourceInverseOf, triple);
					context.getCounter("OWL derived triples", "inverse of")
							.increment(1);
					outputInverseOf(sourceInverseOf.getHistory(), object, subject, inverseOf, alreadyDerived,
							context);
				}
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		sourceFunctional.setRule(Rule.OWL_FUNCTIONAL);
		sourceFunctional.setStep(context.getConfiguration().getInt(
				"reasoner.step", 0));

		sourceInvFunctional.setRule(Rule.OWL_INV_FUNCTIONAL);
		sourceInvFunctional.setStep(context.getConfiguration().getInt(
				"reasoner.step", 0));

		sourceSymmetric.setRule(Rule.OWL_SYMMETRIC);
		sourceSymmetric.setStep(context.getConfiguration().getInt(
				"reasoner.step", 0));

		sourceInverseOf.setRule(Rule.OWL_INVERSE_OF);
		sourceInverseOf.setStep(context.getConfiguration().getInt(
				"reasoner.step", 0));

		if (schemaInverseOfProperties == null) {
			schemaInverseOfProperties = FilesTriplesReader
					.loadCompleteMapIntoMemoryWithInfo(
							"FILTER_ONLY_OWL_INVERSE_OF", context, false);
		}
	}
}
