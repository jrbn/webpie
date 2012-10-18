package reducers.rdfs;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;

import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class RDFSSubpropDomRangeReducer extends
		Reducer<LongWritable, LongWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubpropDomRangeReducer.class);

	protected static Map<Long, Collection<Long>> domainSchemaTriples = null;
	protected static Map<Long, Collection<Long>> rangeSchemaTriples = null;
	protected Set<Long> propURIs = new HashSet<Long>();
	protected Set<Long> derivedProps = new HashSet<Long>();
	private TripleSource source = new TripleSource();
	private Triple oTriple = new Triple();

	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		long uri = key.get();
		derivedProps.clear();

		// Get the predicates with a range or domain associated to this URIs
		propURIs.clear();
		Iterator<LongWritable> itr = values.iterator();
		while (itr.hasNext())
			propURIs.add(itr.next().get());

		Iterator<Long> itrProp = propURIs.iterator();
		while (itrProp.hasNext()) {
			Collection<Long> objects = null;
			long propURI = itrProp.next();
			if ((propURI & 0x1) == 1) {
				objects = rangeSchemaTriples.get(propURI >> 1);
				context.getCounter("derivation", "range matches").increment(1);
			} else {
				objects = domainSchemaTriples.get(propURI >> 1);
				context.getCounter("derivation", "domain matches").increment(1);
			}

			if (objects != null) {
				Iterator<Long> itr3 = objects.iterator();
				while (itr3.hasNext())
					derivedProps.add(itr3.next());
			}
		}

		// Derive the new statements
		Iterator<Long> itr2 = derivedProps.iterator();
		oTriple.setSubject(uri);
		oTriple.setPredicate(TriplesUtils.RDF_TYPE);
		oTriple.setObjectLiteral(false);
		while (itr2.hasNext()) {
			oTriple.setObject(itr2.next());
			context.write(source, oTriple);
		}
		context.getCounter("RDFS derived triples",
				"subprop range and domain rule").increment(derivedProps.size());
	}

	@Override
	public void setup(Context context) throws IOException {

		if (domainSchemaTriples == null) {
			domainSchemaTriples = FilesTriplesReader.loadMapIntoMemory(
					"FILTER_ONLY_DOMAIN_SCHEMA", context);
		}

		if (rangeSchemaTriples == null) {
			rangeSchemaTriples = FilesTriplesReader.loadMapIntoMemory(
					"FILTER_ONLY_RANGE_SCHEMA", context);
		}

		source.setDerivation(TripleSource.RDFS_DOMAIN);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));
	}
}
