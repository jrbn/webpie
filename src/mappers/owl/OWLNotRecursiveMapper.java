package mappers.owl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import utils.NumberUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree.ResourceNode;
import data.Triple;
import data.TripleSource;

public class OWLNotRecursiveMapper
		extends
		Mapper<TripleSource, Triple, BytesWritable, ProtobufWritable<ResourceNode>> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLNotRecursiveMapper.class);

	protected Map<Long, Integer> schemaFunctionalProperties = null;
	protected Map<Long, Integer> schemaInverseFunctionalProperties = null;
	protected Map<Long, Integer> schemaSymmetricProperties = null;
	protected Map<Long, Integer> schemaInverseOfProperties = null;
	private byte[] bKeys = new byte[17];
	private BytesWritable key = new BytesWritable(bKeys);
	private int previousDerivation = -1;

	ProtobufWritable<ResourceNode> oValueContainer = ProtobufWritable
			.newInstance(ResourceNode.class);
	ResourceNode.Builder oValue = ResourceNode.newBuilder();

	@Override
	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Check if the triple has the functional property. If yes output a key
		 * value so it can be matched in the reducer.
		 */
		if (schemaFunctionalProperties.containsKey(value.getPredicate())
				&& !value.isObjectLiteral()) {

			int schemaStep = schemaFunctionalProperties.get(value
					.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousDerivation)
				return;

			// Set as key a particular flag plus the predicate
			bKeys[0] = 0;
			NumberUtils.encodeLong(bKeys, 1, value.getSubject());
			NumberUtils.encodeLong(bKeys, 9, value.getPredicate());

			oValue.setResource(value.getObject());
			oValue.setHistory(key.getHistory());
			oValueContainer.set(oValue.build());
			context.write(this.key, oValueContainer);
		}

		if (schemaInverseFunctionalProperties.containsKey(value.getPredicate())
				&& !value.isObjectLiteral()) {

			int schemaStep = schemaInverseFunctionalProperties.get(value
					.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousDerivation)
				return;

			// Set as key a particular flag plus the predicate
			bKeys[0] = 1;
			NumberUtils.encodeLong(bKeys, 1, value.getObject());
			NumberUtils.encodeLong(bKeys, 9, value.getPredicate());

			oValue.setResource(value.getSubject());
			oValue.setHistory(key.getHistory());
			oValueContainer.set(oValue.build());
			context.write(this.key, oValueContainer);
		}

		if (schemaSymmetricProperties.containsKey(value.getPredicate())) {

			int schemaStep = schemaSymmetricProperties
					.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousDerivation)
				return;

			bKeys[0] = 2;
			NumberUtils.encodeLong(bKeys, 1, value.getSubject());
			NumberUtils.encodeLong(bKeys, 9, value.getObject());

			oValue.setResource(value.getPredicate());
			oValue.setHistory(key.getHistory());
			oValueContainer.set(oValue.build());
			context.write(this.key, oValueContainer);
		}

		if (schemaInverseOfProperties.containsKey(value.getPredicate())) {

			int schemaStep = schemaInverseOfProperties
					.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousDerivation)
				return;

			bKeys[0] = 3;
			NumberUtils.encodeLong(bKeys, 1, value.getSubject());
			NumberUtils.encodeLong(bKeys, 9, value.getObject());

			oValue.setResource(value.getPredicate());
			oValue.setHistory(key.getHistory());
			oValueContainer.set(oValue.build());
			context.write(this.key, oValueContainer);
		}
	}

	@Override
	protected void setup(Context context) throws IOException {
		previousDerivation = context.getConfiguration().getInt(
				"reasoner.previousStep", -1);

		if (schemaFunctionalProperties == null) {
			schemaFunctionalProperties = FilesTriplesReader
					.loadTriplesWithStep("FILTER_ONLY_OWL_FUNCTIONAL_SCHEMA",
							context);
		}

		if (schemaInverseFunctionalProperties == null) {
			schemaInverseFunctionalProperties = FilesTriplesReader
					.loadTriplesWithStep(
							"FILTER_ONLY_OWL_INVERSE_FUNCTIONAL_SCHEMA",
							context);
		}

		if (schemaSymmetricProperties == null) {
			schemaSymmetricProperties = FilesTriplesReader.loadTriplesWithStep(
					"FILTER_ONLY_OWL_SYMMETRIC_SCHEMA", context);
		}

		if (schemaInverseOfProperties == null) {
			schemaInverseOfProperties = FilesTriplesReader.loadTriplesWithStep(
					"FILTER_ONLY_OWL_INVERSE_OF", context);
		}
	}
}
