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

import data.Tree.ByteTwoResourcesNode;
import data.Tree.Node.Rule;
import data.Tree.ResourceNode;
import data.Triple;
import data.TripleSource;

public class OWLHasValueReducer
		extends
		Reducer<LongWritable, ProtobufWritable<ByteTwoResourcesNode>, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLHasValueReducer.class);

	private Triple triple = new Triple();
	private TripleSource sourceHasValue1 = new TripleSource();
	private TripleSource sourceHasValue2 = new TripleSource();

	private Map<Long, Collection<ResourceNode>> hasValueMap = new HashMap<Long, Collection<ResourceNode>>();
	private Map<Long, Collection<ResourceNode>> onPropertyMap = new HashMap<Long, Collection<ResourceNode>>();

	private Map<Long, Collection<ResourceNode>> hasValue2Map = new HashMap<Long, Collection<ResourceNode>>();
	private Map<Long, Collection<ResourceNode>> onProperty2Map = new HashMap<Long, Collection<ResourceNode>>();

	@Override
	public void reduce(LongWritable key,
			Iterable<ProtobufWritable<ByteTwoResourcesNode>> values,
			Context context) throws IOException, InterruptedException {

		for (ProtobufWritable<ByteTwoResourcesNode> value : values) {
			value.setConverter(ByteTwoResourcesNode.class);
			ByteTwoResourcesNode btn = value.get();
			if (btn.getId() == 0) { // Rule 14b
				long object = btn.getResource1();
				Collection<ResourceNode> props = onPropertyMap.get(object);
				if (props != null) {
					Collection<ResourceNode> hasValues = hasValueMap.get(object);
					if (hasValues != null) {
						triple.setSubject(key.get());
						Iterator<ResourceNode> itr2 = props.iterator();
						while (itr2.hasNext()) {
							ResourceNode itr2value = itr2.next();
							long prop = itr2value.getResource();
							triple.setPredicate(prop);
							Iterator<ResourceNode> itr3 = hasValues.iterator();
							while (itr3.hasNext()) {
								ResourceNode itr3value = itr3.next();
								long v = itr3value.getResource();
								triple.setObject(v);
								sourceHasValue1.clearChildren();
								sourceHasValue1.addChild(btn.getHistory());
								sourceHasValue1.addChild(itr2value.getHistory());
								sourceHasValue1.addChild(itr3value.getHistory());
								context.write(sourceHasValue1, triple);
							}
						}
					}
				}
			} else { // Rule 14a
				long predicate = btn.getResource1();
				long object = btn.getResource2();

				Collection<ResourceNode> types = hasValue2Map.get(object);
				Collection<ResourceNode> pred = onProperty2Map.get(predicate);
				if (types != null && pred != null) {
					triple.setSubject(key.get());
					triple.setPredicate(TriplesUtils.RDF_TYPE);
					for(ResourceNode type : types) {
						for(ResourceNode p : pred) {
							if (type.getResource() == p.getResource()) {
								triple.setObject(type.getResource());
								sourceHasValue2.clearChildren();
								sourceHasValue2.addChild(btn.getHistory());
								sourceHasValue2.addChild(type.getHistory());
								sourceHasValue2.addChild(p.getHistory());
								context.write(sourceHasValue2, triple);
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		sourceHasValue1.setRule(Rule.OWL_HAS_VALUE1);
		sourceHasValue1.setStep(context.getConfiguration().getInt(
				"reasoner.step", 0));

		sourceHasValue2.setRule(Rule.OWL_HAS_VALUE2);
		sourceHasValue2.setStep(context.getConfiguration().getInt(
				"reasoner.step", 0));

		triple.setObjectLiteral(false);

		// Load the schema triples
		hasValueMap = FilesTriplesReader.loadCompleteMapIntoMemoryWithInfo(
				"FILTER_ONLY_OWL_HAS_VALUE", context, false);
		onPropertyMap = FilesTriplesReader.loadCompleteMapIntoMemoryWithInfo(
				"FILTER_ONLY_OWL_ON_PROPERTY", context, false);

		hasValue2Map = FilesTriplesReader.loadCompleteMapIntoMemoryWithInfo(
				"FILTER_ONLY_OWL_HAS_VALUE", context, true);
		onProperty2Map = FilesTriplesReader.loadCompleteMapIntoMemoryWithInfo(
				"FILTER_ONLY_OWL_ON_PROPERTY", context, true);
	}
}
