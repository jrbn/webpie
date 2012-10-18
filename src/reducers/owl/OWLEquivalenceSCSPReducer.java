package reducers.owl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import utils.TriplesUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree.ByteResourceNode;
import data.Tree.Node;
import data.Tree.Node.Rule;
import data.Tree.ResourceNode;
import data.Triple;
import data.TripleSource;

public class OWLEquivalenceSCSPReducer
		extends
		Reducer<LongWritable, ProtobufWritable<ByteResourceNode>, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLEquivalenceSCSPReducer.class);

	private TripleSource sourceEqClasses = new TripleSource();
	private TripleSource sourceEqProps = new TripleSource();
	private TripleSource sourceEqClasses2 = new TripleSource();
	private TripleSource sourceEqProps2 = new TripleSource();

	private Triple triple = new Triple();

	protected Map<Long, Collection<ResourceNode>> subpropSchemaTriples = null;
	protected Map<Long, Collection<ResourceNode>> subclassSchemaTriples = null;

	public Map<Long, Node> equivalenceClasses = new HashMap<Long, Node>();
	public Map<Long, Node> superClasses = new HashMap<Long, Node>();

	public Map<Long, Node> equivalenceProperties = new HashMap<Long, Node>();
	public Map<Long, Node> superProperties = new HashMap<Long, Node>();

	Node.Builder nodeBuilder = Node.newBuilder();

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		subclassSchemaTriples = null;
		subpropSchemaTriples = null;
	}

	public Node find(long start, long target) {
		Collection<ResourceNode> superclasses = subclassSchemaTriples
				.get(start);
		if (superclasses != null) {
			for (ResourceNode superclass : superclasses) {
				if (superclass.getResource() == target) {
					return superclass.getHistory();
				} else {
					Node node = find(superclass.getResource(), target);
					if (node != null) {
						nodeBuilder.clearChildren();
						nodeBuilder.addChildren(superclass.getHistory());
						nodeBuilder.addChildren(node);
						return nodeBuilder.build();
					}
				}
			}
		}

		return null;
	}

	@Override
	public void reduce(LongWritable key,
			Iterable<ProtobufWritable<ByteResourceNode>> values, Context context)
			throws IOException, InterruptedException {

		equivalenceClasses.clear();
		superClasses.clear();
		equivalenceProperties.clear();
		superProperties.clear();

		for (ProtobufWritable<ByteResourceNode> value : values) {
			value.setConverter(ByteResourceNode.class);
			ByteResourceNode n = value.get();

			switch (n.getId()) {
			case 0:
				superClasses.put(n.getResource(), n.getHistory());
				break;
			case 1:
				superProperties.put(n.getResource(), n.getHistory());
				break;
			case 2:
				equivalenceClasses.put(n.getResource(), n.getHistory());
				break;
			case 3:
				equivalenceProperties.put(n.getResource(), n.getHistory());
				break;
			default:
				break;
			}
		}

		// Equivalence classes
		if (!equivalenceClasses.isEmpty()) {
			for (Map.Entry<Long, Node> rn : equivalenceClasses.entrySet()) {
				long resource = rn.getKey();
				/*
				 * boolean found = false; Collection<ResourceNode>
				 * existingClasses = subclassSchemaTriples .get(key.get()); if
				 * (existingClasses != null) { Iterator<ResourceNode> exsItr =
				 * existingClasses.iterator(); while (exsItr.hasNext() &&
				 * !found) { if (exsItr.next().getResource() == resource) found
				 * = true; } }
				 */

				if (!superClasses.containsKey(resource)) {
					triple.setSubject(key.get());
					triple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
					triple.setObject(resource);
					sourceEqClasses.clearChildren();
					sourceEqClasses.addChild(rn.getValue());
					context.write(sourceEqClasses, triple);
				}
			}
		}

		// Subclasses
		if (!superClasses.isEmpty()) {
			for (Map.Entry<Long, Node> rn : superClasses.entrySet()) {
				long resource = rn.getKey();
				Node foundNode = find(resource, key.get());
				if (foundNode != null
						&& !equivalenceClasses.containsKey(resource)) {
					triple.setSubject(key.get());
					triple.setPredicate(TriplesUtils.OWL_EQUIVALENT_CLASS);
					triple.setObject(resource);
					sourceEqClasses2.clearChildren();
					sourceEqClasses2.addChild(rn.getValue());
					sourceEqClasses2.addChild(foundNode);
					context.write(sourceEqClasses2, triple);
				}
			}
		}

		// Equivalence properties
		if (!equivalenceProperties.isEmpty()) {
			for (Map.Entry<Long, Node> rn : equivalenceProperties.entrySet()) {
				long resource = rn.getKey();
				boolean found = false;
				Collection<ResourceNode> existingClasses = subpropSchemaTriples
						.get(key.get());
				if (existingClasses != null) {
					Iterator<ResourceNode> exsItr = existingClasses.iterator();
					while (exsItr.hasNext() && !found) {
						if (exsItr.next().getResource() == resource)
							found = true;
					}
				}

				if (!found) {
					triple.setObject(resource);
					triple.setSubject(key.get());
					triple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
					sourceEqProps.clearChildren();
					sourceEqProps.addChild(rn.getValue());
					context.write(sourceEqProps, triple);
				}
			}
		}

		// Subproperties
		if (!superProperties.isEmpty()) {
			for (Map.Entry<Long, Node> rn : superProperties.entrySet()) {
				long resource = rn.getKey();
				boolean found = false;
				Node foundNode = null;
				Collection<ResourceNode> existingClasses = subpropSchemaTriples
						.get(resource);
				if (existingClasses != null) {
					Iterator<ResourceNode> exsItr = existingClasses.iterator();
					while (exsItr.hasNext() && !found) {
						ResourceNode ei = exsItr.next();
						if (ei.getResource() == key.get()) {
							found = true;
							foundNode = ei.getHistory();
						}
					}
				}

				if (found && !equivalenceProperties.containsKey(resource)) {
					triple.setSubject(key.get());
					triple.setPredicate(TriplesUtils.OWL_EQUIVALENT_PROPERTY);
					triple.setObject(resource);
					sourceEqProps2.clearChildren();
					sourceEqProps2.addChild(rn.getValue());
					sourceEqProps2.addChild(foundNode);
					context.write(sourceEqProps2, triple);
				}
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		sourceEqClasses.setRule(Rule.OWL_EQ_CLASSES);
		sourceEqClasses.setAlreadyFiltered(true);
		sourceEqClasses.setStep((byte) context.getConfiguration().getInt(
				"reasoner.step", 0));

		nodeBuilder.setRule(Rule.RDFS_SUBCLASS_TRANS);
		nodeBuilder.setStep((byte) context.getConfiguration().getInt(
				"reasoner.step", 0));


		sourceEqClasses2.setRule(Rule.OWL_SUBCLASS);
		sourceEqClasses2.setAlreadyFiltered(true);
		sourceEqClasses2.setStep((byte) context.getConfiguration().getInt(
				"reasoner.step", 0));

		sourceEqProps.setRule(Rule.OWL_EQ_PROPERTIES);
		sourceEqProps.setAlreadyFiltered(true);
		sourceEqProps.setStep((byte) context.getConfiguration().getInt(
				"reasoner.step", 0));

		sourceEqProps2.setRule(Rule.OWL_SUBPROP);
		sourceEqProps2.setAlreadyFiltered(true);
		sourceEqProps2.setStep((byte) context.getConfiguration().getInt(
				"reasoner.step", 0));

		triple.setObjectLiteral(false);

		if (subpropSchemaTriples == null) {
			subpropSchemaTriples = FilesTriplesReader
					.loadCompleteMapIntoMemoryWithInfo(
							"FILTER_ONLY_SUBPROP_SCHEMA", context, false);
		}

		if (subclassSchemaTriples == null) {
			subclassSchemaTriples = FilesTriplesReader
					.loadCompleteMapIntoMemoryWithInfo(
							Rule.RDFS_SUBCLASS_TRANS,
							"FILTER_ONLY_SUBCLASS_SCHEMA", context, false);
		}
	}
}
