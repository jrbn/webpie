package reducers.owl;

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

public class OWLEquivalenceSCSPReducer extends
		Reducer<LongWritable, BytesWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLEquivalenceSCSPReducer.class);

	private TripleSource source = new TripleSource();
	private Triple triple = new Triple();

	protected static Map<Long, Collection<Long>> subpropSchemaTriples = null;
	public static Map<Long, Collection<Long>> subclassSchemaTriples = null;

	public Set<Long> equivalenceClasses = new HashSet<Long>();
	public Set<Long> superClasses = new HashSet<Long>();

	public Set<Long> equivalenceProperties = new HashSet<Long>();
	public Set<Long> superProperties = new HashSet<Long>();

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {

		equivalenceClasses.clear();
		superClasses.clear();
		equivalenceProperties.clear();
		superProperties.clear();

		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable value = itr.next();
			byte[] bValue = value.getBytes();
			long resource = NumberUtils.decodeLong(bValue, 1);
			switch (bValue[0]) {
			case 0:
				superClasses.add(resource);
				break;
			case 1:
				superProperties.add(resource);
				break;
			case 2:
				equivalenceClasses.add(resource);
				break;
			case 3:
				equivalenceProperties.add(resource);
				break;
			default:
				break;
			}
		}

		// Equivalence classes
		Iterator<Long> itr2 = equivalenceClasses.iterator();
		while (itr2.hasNext()) {
			long resource = itr2.next();
			boolean found = false;
			Collection<Long> existingClasses = subclassSchemaTriples.get(key
					.get());
			if (existingClasses != null) {
				Iterator<Long> exsItr = existingClasses.iterator();
				while (exsItr.hasNext() && !found) {
					if (exsItr.next() == resource)
						found = true;
				}
			}

			if (!found) {
				triple.setObject(resource);
				triple.setSubject(key.get());
				triple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
				context.write(source, triple);
			}
		}

		// Equivalence properties
		itr2 = equivalenceProperties.iterator();
		while (itr2.hasNext()) {
			long resource = itr2.next();
			boolean found = false;
			Collection<Long> existingClasses = subpropSchemaTriples.get(key
					.get());
			if (existingClasses != null) {
				Iterator<Long> exsItr = existingClasses.iterator();
				while (exsItr.hasNext() && !found) {
					if (exsItr.next() == resource)
						found = true;
				}
			}

			if (!found) {
				triple.setObject(resource);
				triple.setSubject(key.get());
				triple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
				context.write(source, triple);
			}
		}

		// Subproperties
		itr2 = superProperties.iterator();
		while (itr2.hasNext()) {
			long resource = itr2.next();
			boolean found = false;
			Collection<Long> existingClasses = subpropSchemaTriples
					.get(resource);
			if (existingClasses != null) {
				Iterator<Long> exsItr = existingClasses.iterator();
				while (exsItr.hasNext() && !found) {
					if (exsItr.next() == key.get())
						found = true;
				}
			}

			if (found && !equivalenceProperties.contains(resource)) {
				triple.setSubject(key.get());
				triple.setPredicate(TriplesUtils.OWL_EQUIVALENT_PROPERTY);
				triple.setObject(resource);
				context.write(source, triple);
			}
		}

		// Subclasses
		itr2 = superClasses.iterator();
		while (itr2.hasNext()) {
			long resource = itr2.next();
			boolean found = false;
			Collection<Long> existingClasses = subclassSchemaTriples
					.get(resource);
			if (existingClasses != null) {
				Iterator<Long> exsItr = existingClasses.iterator();
				while (exsItr.hasNext() && !found) {
					if (exsItr.next() == key.get())
						found = true;
				}
			}

			if (found && !equivalenceClasses.contains(resource)) {
				triple.setSubject(key.get());
				triple.setPredicate(TriplesUtils.OWL_EQUIVALENT_CLASS);
				triple.setObject(resource);
				context.write(source, triple);
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		source.setDerivation(TripleSource.OWL_RULE_12A);
		source.setAlreadyFiltered(true);
		source.setStep((byte) context.getConfiguration().getInt(
				"reasoner.step", 0));
		triple.setObjectLiteral(false);

		if (subpropSchemaTriples == null) {
			subpropSchemaTriples = FilesTriplesReader.loadMapIntoMemory(
					"FILTER_ONLY_SUBPROP_SCHEMA", context);
		}

		if (subclassSchemaTriples == null) {
			subclassSchemaTriples = FilesTriplesReader.loadMapIntoMemory(
					"FILTER_ONLY_SUBCLASS_SCHEMA", context);
		}
	}
}
