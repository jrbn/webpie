package reducers.rdfs;

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
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class RDFSSubpropInheritReducer extends
		Reducer<BytesWritable, LongWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubpropInheritReducer.class);

	protected static Map<Long, Collection<Long>> subpropSchemaTriples = null;
	protected Set<Long> propURIs = new HashSet<Long>();
	protected Set<Long> derivedProps = new HashSet<Long>();
	private TripleSource source = new TripleSource();

	private Triple oTriple = new Triple();
	private Triple oTriple2 = new Triple();

	private void recursiveScanSubproperties(long value, Set<Long> set) {
		Collection<Long> subprops = subpropSchemaTriples.get(value);
		if (subprops != null) {
			Iterator<Long> itr = subprops.iterator();
			while (itr.hasNext()) {
				long subprop = itr.next();
				if (!set.contains(subprop)) {
					set.add(subprop);
					recursiveScanSubproperties(subprop, set);
				}
			}
		}
	}

	@Override
	public void reduce(BytesWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		byte[] bKey = key.getBytes();

		switch (bKey[0]) {
		case 2:
		case 3:
			// subprop inheritance
			long subject = NumberUtils.decodeLong(bKey, 1);
			long uri = NumberUtils.decodeLong(bKey, 9);
			propURIs.clear();
			// filter the properties that are already present
			Iterator<LongWritable> itr = values.iterator();
			while (itr.hasNext()) {
				long value = itr.next().get();
				if (!propURIs.contains(value)) {
					recursiveScanSubproperties(value, propURIs);
				}
			}

			Iterator<Long> itr3 = propURIs.iterator();
			boolean isLiteral = bKey[0] == 3;
			oTriple.setSubject(subject);
			oTriple.setObject(uri);
			oTriple.setObjectLiteral(isLiteral);
			while (itr3.hasNext()) {
				oTriple.setPredicate(itr3.next());
				context.write(source, oTriple);
			}
			context.getCounter("RDFS derived triples",
					"subprop inheritance rule").increment(propURIs.size());
			break;
		case 5:
			// Subproperty transitivity
			subject = NumberUtils.decodeLong(bKey, 1);
			propURIs.clear();
			// filter the properties that are already present
			Iterator<LongWritable> itr2 = values.iterator();
			while (itr2.hasNext()) {
				long value = itr2.next().get();
				if (!propURIs.contains(value)) {
					recursiveScanSubproperties(value, propURIs);
				}
			}

			Iterator<Long> itr4 = propURIs.iterator();
			oTriple.setSubject(subject);
			oTriple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
			oTriple.setObjectLiteral(false);
			while (itr4.hasNext()) {
				oTriple.setObject(itr4.next());
				context.write(source, oTriple);
			}
			context.getCounter("RDFS derived triples",
					"subprop transitivity rule").increment(propURIs.size());

			break;
		default:
			break;
		}
	}

	@Override
	public void setup(Context context) throws IOException {

		if (subpropSchemaTriples == null) {
			subpropSchemaTriples = FilesTriplesReader.loadMapIntoMemory(
					"FILTER_ONLY_SUBPROP_SCHEMA", context);
		}

		source.setDerivation(TripleSource.RDFS_SUBPROP_INHERIT);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));

		oTriple2.setPredicate(TriplesUtils.RDF_TYPE);
		oTriple2.setObjectLiteral(false);
	}
}
