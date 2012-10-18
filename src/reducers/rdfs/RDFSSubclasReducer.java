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

public class RDFSSubclasReducer extends
		Reducer<BytesWritable, LongWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubclasReducer.class);

	public static Map<Long, Collection<Long>> subclassSchemaTriples = null;
	protected Set<Long> subclasURIs = new HashSet<Long>();
	protected Set<Long> existingURIs = new HashSet<Long>();
	protected Set<Long> memberProperties = null;
	protected Set<Long> specialSuperclasses = new HashSet<Long>();
	private TripleSource source = new TripleSource();
	private Triple oTriple = new Triple();

	private void recursiveScanSuperclasses(long value, Set<Long> set) {
		Collection<Long> subclassValues = subclassSchemaTriples.get(value);
		if (subclassValues != null) {
			Iterator<Long> itr = subclassValues.iterator();
			while (itr.hasNext()) {
				long classValue = itr.next();
				if (!set.contains(classValue)) {
					set.add(classValue);
					recursiveScanSuperclasses(classValue, set);
				}
			}
		}
	}

	@Override
	public void reduce(BytesWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		existingURIs.clear();

		Iterator<LongWritable> itr = values.iterator();
		while (itr.hasNext()) {
			long value = itr.next().get();
			existingURIs.add(value);
		}

		Iterator<Long> oTypes = existingURIs.iterator();
		subclasURIs.clear();
		while (oTypes.hasNext()) {
			long existingURI = oTypes.next();
			recursiveScanSuperclasses(existingURI, subclasURIs);
		}

		subclasURIs.removeAll(existingURIs);

		oTypes = subclasURIs.iterator();
		byte[] bKey = key.getBytes();
		long oKey = NumberUtils.decodeLong(bKey, 1);
		oTriple.setSubject(oKey);
		boolean typeTriple = bKey[0] == 0;
		if (!typeTriple) { // It's a subclass triple
			oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
		} else { // It's a type triple
			oTriple.setPredicate(TriplesUtils.RDF_TYPE);
		}
		while (oTypes.hasNext()) {
			long oType = oTypes.next();
			oTriple.setObject(oType);
			context.write(source, oTriple);
		}

		if (typeTriple) {
			/* Check special rules */
			if ((subclasURIs
					.contains(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY) || existingURIs
					.contains(TriplesUtils.RDFS_CONTAINER_MEMBERSHIP_PROPERTY))
					&& !memberProperties.contains(oTriple.getSubject())) {
				oTriple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
				oTriple.setObject(TriplesUtils.RDFS_MEMBER);
				context.write(source, oTriple);
				context.getCounter("RDFS derived triples",
						"subproperty of member").increment(1);
			}

			if (subclasURIs.contains(TriplesUtils.RDFS_DATATYPE)
					|| existingURIs.contains(TriplesUtils.RDFS_DATATYPE)) {
				specialSuperclasses.clear();
				recursiveScanSuperclasses(oTriple.getSubject(),
						specialSuperclasses);
				if (!specialSuperclasses.contains(TriplesUtils.RDFS_LITERAL)) {
					oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
					oTriple.setObject(TriplesUtils.RDFS_LITERAL);
					context.write(source, oTriple);
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
			subclassSchemaTriples = FilesTriplesReader.loadMapIntoMemory(
					"FILTER_ONLY_SUBCLASS_SCHEMA", context);
		}

		if (memberProperties == null) {
			memberProperties = new HashSet<Long>();
			FilesTriplesReader.loadSetIntoMemory(memberProperties, context,
					"FILTER_ONLY_MEMBER_SUBPROP_SCHEMA", -1);
		}

		source.setDerivation(TripleSource.RDFS_SUBCLASS_INHERIT);
		source.setAlreadyFiltered(true);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));
	}
}
