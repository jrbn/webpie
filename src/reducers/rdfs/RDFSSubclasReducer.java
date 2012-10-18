package reducers.rdfs;

import java.io.IOException;
import java.util.ArrayList;
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
import utils.TriplesUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree;
import data.Tree.Node;
import data.Tree.Node.Rule;
import data.Tree.ResourceNode;
import data.Triple;
import data.TripleSource;

public class RDFSSubclasReducer
		extends
		Reducer<BytesWritable, ProtobufWritable<Tree.ResourceNode>, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubclasReducer.class);

	public static Map<Long, Collection<ResourceNode>> subclassSchemaTriples = null;
	protected Map<Long, Node> subclasURIs = new HashMap<Long, Node>();
	protected Map<Long, Node> existingURIs = new HashMap<Long, Node>();
	protected Set<Long> memberProperties = null;
	protected Set<Long> specialSuperclasses = new HashSet<Long>();

	private TripleSource sourceMember = new TripleSource();
	private TripleSource sourceDatatype = new TripleSource();
	private TripleSource sourceIhn = new TripleSource();
	private TripleSource sourceTrans = new TripleSource();

	private Triple oTriple = new Triple();

	private void recursiveScanSuperclasses(Context context,
			TripleSource source, long value, Node historyValue,
			Set<Long> existingSet, Map<Long, Node> derivedSet)
			throws IOException, InterruptedException {

		Collection<ResourceNode> subclassValues = subclassSchemaTriples
				.get(value);

		if (subclassValues != null) {
			Iterator<ResourceNode> itr = subclassValues.iterator();
			while (itr.hasNext()) {
				ResourceNode classValue = itr.next();
				if (!derivedSet.containsKey(classValue.getResource())
						&& !existingSet.contains(classValue.getResource())) {

					source.clearChildren();
					source.addChild(historyValue);
					source.addChild(classValue.getHistory());
					oTriple.setObject(classValue.getResource());

					context.write(source, oTriple);

					derivedSet.put(classValue.getResource(),
							source.getHistory());
					recursiveScanSuperclasses(context, source,
							classValue.getResource(), source.getHistory(),
							existingSet, derivedSet);
				}
			}
		}
	}

	@Override
	public void reduce(BytesWritable key,
			Iterable<ProtobufWritable<Tree.ResourceNode>> values,
			Context context) throws IOException, InterruptedException {

		byte[] bKey = key.getBytes();
		long oKey = NumberUtils.decodeLong(bKey, 1);
		oTriple.setSubject(oKey);
		boolean typeTriple = bKey[0] == 0;
		TripleSource source = null;
		if (!typeTriple) { // It's a subclass triple
			oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
			source = sourceTrans;
		} else { // It's a type triple
			oTriple.setPredicate(TriplesUtils.RDF_TYPE);
			source = sourceIhn;
		}

		//Get existing superclasses
		existingURIs.clear();
		for(ProtobufWritable<Tree.ResourceNode> value : values) {
			value.setConverter(ResourceNode.class);
			ResourceNode n = value.get();
			existingURIs.put(n.getResource(), n.getHistory());
		}

		subclasURIs.clear();
		for (Map.Entry<Long, Node> entry : existingURIs.entrySet()) {
			recursiveScanSuperclasses(context, source, entry.getKey(),
					entry.getValue(), existingURIs.keySet(), subclasURIs);
		}

		if (typeTriple) {
			if ((subclasURIs
					.containsKey(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY) || existingURIs
					.containsKey(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY))
					&& !memberProperties.contains(oTriple.getSubject())) {
				sourceMember.clearChildren();
				if (existingURIs
						.containsKey(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY)) {
					sourceMember
							.addChild(existingURIs
									.get(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY));
				} else {
					sourceMember
							.addChild(subclasURIs
									.get(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY));
				}
				oTriple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
				oTriple.setObject(TriplesUtils.RDFS_MEMBER);
				context.write(sourceMember, oTriple);
				context.getCounter("RDFS derived triples",
						"subproperty of member").increment(1);
			}

			if (subclasURIs.containsKey(TriplesUtils.RDFS_DATATYPE)
					|| existingURIs.containsKey(TriplesUtils.RDFS_DATATYPE)) {
				sourceDatatype.clearChildren();

				ArrayList<Long> list = new ArrayList<Long>();
				list.add(oTriple.getSubject());
				boolean found = false;
				while (!list.isEmpty() && !found) {
					long resource = list.remove(0);
					if (resource == TriplesUtils.RDFS_LITERAL) {
						found = true;
					} else { // Add the children
						Collection<ResourceNode> superclasses = subclassSchemaTriples
								.get(resource);
						if (superclasses != null) {
							for (ResourceNode node : superclasses) {
								list.add(node.getResource());
							}
						}
					}
				}

				if (!found) {
					oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
					oTriple.setObject(TriplesUtils.RDFS_LITERAL);

					sourceDatatype.clearChildren();
					if (existingURIs.containsKey(TriplesUtils.RDFS_DATATYPE)) {
						sourceDatatype.addChild(existingURIs
								.get(TriplesUtils.RDFS_DATATYPE));
					} else {
						sourceDatatype.addChild(subclasURIs
								.get(TriplesUtils.RDFS_DATATYPE));
					}
					context.write(sourceDatatype, oTriple);
					context.getCounter("RDFS derived triples",
							"subclass of Literal").increment(1);
				}
			}
		}

		// Update the counters
		if (typeTriple)
			context.getCounter("RDFS derived triples",
					"subclass inheritance rule").increment(subclasURIs.size());
		else
			context.getCounter("RDFS derived triples",
					"subclass transitivity rule").increment(subclasURIs.size());
	}

	@Override
	public void setup(Context context) throws IOException {

		if (subclassSchemaTriples == null) {
			subclassSchemaTriples = FilesTriplesReader
					.loadCompleteMapIntoMemoryWithInfo(
							Rule.RDFS_SUBCLASS_TRANS,
							"FILTER_ONLY_SUBCLASS_SCHEMA", context, false);
		}

		if (memberProperties == null) {
			memberProperties = new HashSet<Long>();
			FilesTriplesReader.loadSetIntoMemory(memberProperties, context,
					"FILTER_ONLY_MEMBER_SUBPROP_SCHEMA", -1);
		}

		sourceIhn.setRule(Rule.RDFS_SUBCLASS_INHERIT);
		sourceIhn.setAlreadyFiltered(true);
		sourceIhn
				.setStep(context.getConfiguration().getInt("reasoner.step", 0));

		sourceMember.setRule(Rule.RDFS_MEMBERSHIP_PROP);
		sourceMember.setAlreadyFiltered(true);
		sourceMember.setStep(context.getConfiguration().getInt("reasoner.step",
				0));

		sourceDatatype.setRule(Rule.RDFS_DATATYPE);
		sourceDatatype.setAlreadyFiltered(true);
		sourceDatatype.setStep(context.getConfiguration().getInt(
				"reasoner.step", 0));

		sourceTrans.setRule(Rule.RDFS_SUBCLASS_TRANS);
		sourceTrans.setAlreadyFiltered(true);
		sourceTrans.setStep(context.getConfiguration().getInt("reasoner.step",
				0));
	}
}
