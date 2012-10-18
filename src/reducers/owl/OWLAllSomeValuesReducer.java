package reducers.owl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.TriplesUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree.ByteResourceNode;
import data.Tree.Node;
import data.Tree.Node.Rule;
import data.Triple;
import data.TripleSource;

public class OWLAllSomeValuesReducer
		extends
		Reducer<BytesWritable, ProtobufWritable<ByteResourceNode>, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLAllSomeValuesReducer.class);
	private Triple triple = new Triple();

	private TripleSource sourceSome = new TripleSource();
	private TripleSource sourceAll = new TripleSource();

	private Collection<ByteResourceNode> types = new ArrayList<ByteResourceNode>();
	private Collection<ByteResourceNode> resources = new ArrayList<ByteResourceNode>();
	private int previousDerivation = -1;

	@Override
	public void reduce(BytesWritable key,
			Iterable<ProtobufWritable<ByteResourceNode>> values, Context context)
			throws IOException, InterruptedException {
		types.clear();
		resources.clear();

		Iterator<ProtobufWritable<ByteResourceNode>> itr = values.iterator();
		while (itr.hasNext()) {
			ProtobufWritable<ByteResourceNode> brn = itr.next();
			brn.setConverter(ByteResourceNode.class);
			ByteResourceNode value = brn.get();
			if (value.getId() == 1) { // Type triple
				types.add(value);
			} else { // Resource triple
				resources.add(value);
			}
		}

		for (ByteResourceNode resource : resources) {
			triple.setSubject(resource.getResource());
			for (ByteResourceNode type : types) {
				triple.setObject(type.getResource());
				if (Math.max(resource.getHistory().getStep(), type.getHistory()
						.getStep()) >= (previousDerivation - 1)) {
					// Fix the history of the source
					sourceSome.clearChildren();
					sourceSome.addChild(resource.getHistory());
					for (Node child : type.getHistory().getChildrenList())
						sourceSome.addChild(child);
					context.write(sourceSome, triple);
				}
			}
		}
	}

	@Override
	public void setup(Context context) {
		previousDerivation = context.getConfiguration().getInt(
				"reasoner.previousStep", -1);
		sourceSome.setRule(Rule.OWL_SOME_VALUES);
		sourceSome.setStep(context.getConfiguration()
				.getInt("reasoner.step", 0));
		sourceAll.setRule(Rule.OWL_ALL_VALUES);
		sourceAll
				.setStep(context.getConfiguration().getInt("reasoner.step", 0));
		triple.setObjectLiteral(false);
		triple.setPredicate(TriplesUtils.RDF_TYPE);
	}
}
