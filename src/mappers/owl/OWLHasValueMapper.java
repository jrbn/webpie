package mappers.owl;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import utils.TriplesUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree.ByteTwoResourcesNode;
import data.Triple;
import data.TripleSource;

public class OWLHasValueMapper extends
		Mapper<TripleSource, Triple, LongWritable, ProtobufWritable<ByteTwoResourcesNode>> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLHasValueMapper.class);

	private LongWritable oKey = new LongWritable();

	private Map<Long, Integer> hasValue = null;
	private Map<Long, Integer> hasValueInverted = null;
	private Map<Long, Integer> onProperty = null;
	private Map<Long, Integer> onPropertyInverted = null;
	private int previousStep = -1;

	private ProtobufWritable<ByteTwoResourcesNode> oValueContainer = ProtobufWritable
			.newInstance(ByteTwoResourcesNode.class);
	protected ByteTwoResourcesNode.Builder oValue = ByteTwoResourcesNode.newBuilder();

	@Override
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

			oValue.setId(0);
			oValue.setResource1(value.getObject());
			oValue.setHistory(key.getHistory());
			oValueContainer.set(oValue.build());
			context.write(oKey, oValueContainer);

		} else if (value.getPredicate() != TriplesUtils.RDF_TYPE
				&& hasValueInverted.containsKey(value.getObject())
				&& onPropertyInverted.containsKey(value.getPredicate())) {

			int schemaHasValueStep = hasValueInverted.get(value.getObject());
			int schemaOnPropertyStep = onPropertyInverted.get(value.getPredicate());
			if (Math.max(Math.max(schemaHasValueStep, schemaOnPropertyStep),
					key.getStep()) < previousStep - 1)
				return;

			oValue.setId(1);
			oValue.setResource1(value.getPredicate());
			oValue.setResource2(value.getObject());
			oValue.setHistory(key.getHistory());
			oValueContainer.set(oValue.build());
			context.write(oKey, oValueContainer);
		}
	}

	@Override
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
