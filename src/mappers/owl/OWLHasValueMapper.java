package mappers.owl;

import java.io.IOException;
import java.util.Map;

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

public class OWLHasValueMapper extends
		Mapper<TripleSource, Triple, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLHasValueMapper.class);

	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	private byte[] values = new byte[17];

	private Map<Long, Integer> hasValue = null;
	private Map<Long, Integer> hasValueInverted = null;
	private Map<Long, Integer> onProperty = null;
	private Map<Long, Integer> onPropertyInverted = null;
	private int previousStep = -1;

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		oKey.set(value.getSubject());
		if (value.getPredicate() == TriplesUtils.RDF_TYPE
				&& hasValue.containsKey(value.getObject())
				&& onProperty.containsKey(value.getObject())) {

			int schemaHasValueStep = hasValue.get(value.getObject());
			int schemaOnPropertyStep = onProperty.get(value.getObject());
			if (Math.max(Math.max(schemaHasValueStep, schemaOnPropertyStep),
					key.getStep()) < previousStep - 1)
				return;

			values[0] = 0;
			NumberUtils.encodeLong(values, 1, value.getObject());
			oValue.set(values, 0, 9);
			context.write(oKey, oValue);
		} else if (value.getPredicate() != TriplesUtils.RDF_TYPE
				&& hasValueInverted.containsKey(value.getObject())
				&& onPropertyInverted.containsKey(value.getPredicate())) {

			int schemaHasValueStep = hasValueInverted.get(value.getObject());
			int schemaOnPropertyStep = onPropertyInverted.get(value
					.getPredicate());
			if (Math.max(Math.max(schemaHasValueStep, schemaOnPropertyStep),
					key.getStep()) < previousStep - 1)
				return;

			values[0] = 1;
			NumberUtils.encodeLong(values, 1, value.getPredicate());
			NumberUtils.encodeLong(values, 9, value.getObject());
			oValue.set(values, 0, 17);
			context.write(oKey, oValue);
		}
	}

	public void setup(Context context) throws IOException {
		previousStep = context.getConfiguration().getInt(
				"reasoner.previousStep", -1);

		if (hasValue == null) {
			hasValue = FilesTriplesReader.loadTriplesWithStep(
					"FILTER_ONLY_OWL_HAS_VALUE", context);
			hasValueInverted = FilesTriplesReader.loadTriplesWithStep(
					"FILTER_ONLY_OWL_HAS_VALUE", context, true);
		}

		if (onProperty == null) {
			onProperty = FilesTriplesReader.loadTriplesWithStep(
					"FILTER_ONLY_OWL_ON_PROPERTY", context);
			onPropertyInverted = FilesTriplesReader.loadTriplesWithStep(
					"FILTER_ONLY_OWL_ON_PROPERTY", context, true);
		}
	}
}
