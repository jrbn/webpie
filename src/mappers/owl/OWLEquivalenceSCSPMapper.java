package mappers.owl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWLEquivalenceSCSPMapper extends
		Mapper<TripleSource, Triple, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLEquivalenceSCSPMapper.class);
	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	private byte[] bValues = new byte[17];

	private static Set<Long> subclassSchemaTriples = null;
	private static Set<Long> subpropSchemaTriples = null;

	@Override
	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		if (value.getSubject() == value.getObject())
			return;

		oKey.set(value.getSubject());
		NumberUtils.encodeLong(bValues, 1, value.getObject());

		if (value.getPredicate() == TriplesUtils.RDFS_SUBCLASS
				&& subclassSchemaTriples.contains(value.getObject())) {
			bValues[0] = 0;
			oValue.set(bValues, 0, 9);
			context.write(oKey, oValue);
		}

		if (value.getPredicate() == TriplesUtils.OWL_EQUIVALENT_CLASS) {
			bValues[0] = 2;
			oValue.set(bValues, 0, 9);
			context.write(oKey, oValue);

			oKey.set(value.getObject());
			NumberUtils.encodeLong(bValues, 1, value.getSubject());
			oValue.set(bValues, 0, 9);
			context.write(oKey, oValue);
		}

		if (value.getPredicate() == TriplesUtils.RDFS_SUBPROPERTY
				&& subpropSchemaTriples.contains(value.getObject())) {
			bValues[0] = 1;
			oValue.set(bValues, 0, 9);
			context.write(oKey, oValue);
		}

		if (value.getPredicate() == TriplesUtils.OWL_EQUIVALENT_PROPERTY) {
			bValues[0] = 3;
			oValue.set(bValues, 0, 9);
			context.write(oKey, oValue);

			oKey.set(value.getObject());
			NumberUtils.encodeLong(bValues, 1, value.getSubject());
			oValue.set(bValues, 0, 9);
			context.write(oKey, oValue);
		}
	}

	@Override
	public void setup(Context context) throws IOException {

		if (subpropSchemaTriples == null) {
			subpropSchemaTriples = new HashSet<Long>();
			FilesTriplesReader.loadSetIntoMemory(subpropSchemaTriples, context,
					"FILTER_ONLY_SUBPROP_SCHEMA", -1);
		}

		if (subclassSchemaTriples == null) {
			subclassSchemaTriples = new HashSet<Long>();
			FilesTriplesReader.loadSetIntoMemory(subclassSchemaTriples,
					context, "FILTER_ONLY_SUBCLASS_SCHEMA", -1);
		}
	}
}
