package reducers.rdfs;

import java.io.IOException;
import java.util.Arrays;
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
import utils.TriplesUtils;
import data.Tree.Node;
import data.Tree.Node.Rule;
import data.Tree.ResourceNode;
import data.Triple;
import data.TripleSource;

public class RDFSSubpropDomRangeReducer extends
		Reducer<LongWritable, BytesWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubpropDomRangeReducer.class);

	protected static Map<Long, Collection<ResourceNode>> domainSchemaTriples = null;
	protected static Map<Long, Collection<ResourceNode>> rangeSchemaTriples = null;
	protected Set<Long> processedDomainURIs = new HashSet<Long>();
	protected Set<Long> processedRangeURIs = new HashSet<Long>();
	protected Set<Long> derivedProps = new HashSet<Long>();

	private TripleSource sourceDomain = new TripleSource();
	private TripleSource sourceRange = new TripleSource();

	private Triple oTriple = new Triple();

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {

		long uri = key.get();
		oTriple.setSubject(uri);
		derivedProps.clear();
		processedDomainURIs.clear();
		processedRangeURIs.clear();
		TripleSource source = null;

		// Get the predicates with a range or domain associated to this URIs
		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable value = itr.next();
			long toMatch = NumberUtils.decodeLong(value.getBytes(), 1);

			Collection<ResourceNode> objects = null;
			if (value.getBytes()[0] == 1
					&& !processedRangeURIs.contains(toMatch)) {
				objects = rangeSchemaTriples.get(toMatch);
				source = sourceRange;
				processedRangeURIs.add(toMatch);
			} else if (value.getBytes()[0] == 0
					&& !processedDomainURIs.contains(toMatch)) {
				objects = domainSchemaTriples.get(toMatch);
				source = sourceDomain;
				processedDomainURIs.add(toMatch);
			}

			if (objects != null) {
				for (ResourceNode object : objects) {
					if (!derivedProps.contains(object.getResource())) {
						derivedProps.add(object.getResource());
						//Proceed with the derivation
						oTriple.setObject(object.getResource());

						source.clearChildren();

						byte[] history = Arrays.copyOfRange(value.getBytes(), 9, value.getLength());
						source.addChild(Node.parseFrom(history));
						source.addChild(object.getHistory());
						context.write(source, oTriple);
					}
				}
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {

		if (domainSchemaTriples == null) {
			domainSchemaTriples = FilesTriplesReader
					.loadCompleteMapIntoMemoryWithInfo(
							"FILTER_ONLY_DOMAIN_SCHEMA", context, false);
		}

		if (rangeSchemaTriples == null) {
			rangeSchemaTriples = FilesTriplesReader
					.loadCompleteMapIntoMemoryWithInfo(
							"FILTER_ONLY_RANGE_SCHEMA", context, false);
		}

		oTriple.setObjectLiteral(false);
		oTriple.setPredicate(TriplesUtils.RDF_TYPE);
		sourceDomain.setRule(Rule.RDFS_DOMAIN);
		sourceDomain.setStep(context.getConfiguration().getInt("reasoner.step",
				0));
		sourceRange.setRule(Rule.RDFS_RANGE);
		sourceRange.setStep(context.getConfiguration().getInt("reasoner.step",
				0));
	}
}
