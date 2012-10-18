package reducers.rdfs;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
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

import data.Tree.Node;
import data.Tree.Node.Rule;
import data.Tree.ResourceNode;
import data.Triple;
import data.TripleSource;

public class RDFSSubpropInheritReducer
		extends
		Reducer<BytesWritable, ProtobufWritable<ResourceNode>, TripleSource, Triple> {

	private static Logger log = LoggerFactory
			.getLogger(RDFSSubpropInheritReducer.class);

	protected static Map<Long, Collection<ResourceNode>> subpropSchemaTriples = null;
	protected Set<Long> alreadyDerived = new HashSet<Long>();
	protected LinkedList<ResourceNode> toProcess = new LinkedList<ResourceNode>();

	private TripleSource sourceIhn = new TripleSource();
	private TripleSource sourceTrans = new TripleSource();
	private Triple oTriple = new Triple();

	private void recursiveScanSuperproperties(Context context,
			TripleSource source, long value, Node historyValue,
			Set<Long> derivedSet, boolean predicate) throws IOException,
			InterruptedException {

		Collection<ResourceNode> subpropsValues = subpropSchemaTriples
				.get(value);

		if (subpropsValues != null) {
			Iterator<ResourceNode> itr = subpropsValues.iterator();
			while (itr.hasNext()) {
				ResourceNode classValue = itr.next();
				if (!derivedSet.contains(classValue.getResource())) {

					source.clearChildren();
					source.addChild(historyValue);
					source.addChild(classValue.getHistory());
					if (predicate) {
						oTriple.setPredicate(classValue.getResource());
					} else {
						oTriple.setObject(classValue.getResource());
					}

					context.write(source, oTriple);

					derivedSet.add(classValue.getResource());
					recursiveScanSuperproperties(context, source,
							classValue.getResource(), source.getHistory(),
							derivedSet, predicate);
				}
			}
		}
	}

	@Override
	public void reduce(BytesWritable key,
			Iterable<ProtobufWritable<ResourceNode>> values, Context context)
			throws IOException, InterruptedException {
		byte[] bKey = key.getBytes();

		switch (bKey[0]) {
		case 2:
		case 3:
			// subprop inheritance
			long subject = NumberUtils.decodeLong(bKey, 1);
			long uri = NumberUtils.decodeLong(bKey, 9);
			boolean isLiteral = bKey[0] == 3;

			oTriple.setSubject(subject);
			oTriple.setObject(uri);
			oTriple.setObjectLiteral(isLiteral);

			alreadyDerived.clear();

			// filter the properties that are already present
			for (ProtobufWritable<ResourceNode> wrapper : values) {
				wrapper.setConverter(ResourceNode.class);
				ResourceNode rn = wrapper.get();
				alreadyDerived.add(rn.getResource());
				recursiveScanSuperproperties(context, sourceIhn,
						rn.getResource(), rn.getHistory(), alreadyDerived, true);
			}
			break;
		case 5:
			// Subproperty transitivity
			subject = NumberUtils.decodeLong(bKey, 1);

			oTriple.setSubject(subject);
			oTriple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);

			alreadyDerived.clear();
			for (ProtobufWritable<ResourceNode> wrapper : values) {
				wrapper.setConverter(ResourceNode.class);
				ResourceNode rn = wrapper.get();
				alreadyDerived.add(rn.getResource());
				recursiveScanSuperproperties(context, sourceTrans,
						rn.getResource(), rn.getHistory(), alreadyDerived,
						false);
			}
			break;
		default:
			break;
		}
	}

	@Override
	public void setup(Context context) throws IOException {

		subpropSchemaTriples = FilesTriplesReader
				.loadCompleteMapIntoMemoryWithInfo(
						"FILTER_ONLY_SUBPROP_SCHEMA", context, false);

		sourceIhn.setRule(Rule.RDFS_SUBPROP_INHERIT);
		sourceIhn
				.setStep(context.getConfiguration().getInt("reasoner.step", 0));

		sourceTrans.setRule(Rule.RDFS_SUBPROP_TRANS);
		sourceTrans.setStep(context.getConfiguration().getInt("reasoner.step",
				0));
	}
}