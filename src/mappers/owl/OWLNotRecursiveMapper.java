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
import data.Triple;
import data.TripleSource;

public class OWLNotRecursiveMapper extends
		Mapper<TripleSource, Triple, BytesWritable, LongWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLNotRecursiveMapper.class);

	protected Map<Long, Integer> schemaFunctionalProperties = null;
	protected Map<Long, Integer> schemaInverseFunctionalProperties = null;
	protected Map<Long, Integer> schemaSymmetricProperties = null;
	protected Map<Long, Integer> schemaInverseOfProperties = null;
	private byte[] bKeys = new byte[17];
	private BytesWritable key = new BytesWritable(bKeys);
	private int previousDerivation = -1;

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
			context.write(this.key, new LongWritable(value.getObject()));
		}

		if (schemaInverseFunctionalProperties.containsKey(value.getPredicate())
				&& !value.isObjectLiteral()) {

			int schemaStep = schemaInverseFunctionalProperties.get(value
					.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousDerivation)
				return;

			// Set as key a particular flag plus the predicate
			bKeys[0] = 0;
			NumberUtils.encodeLong(bKeys, 1, value.getObject());
			NumberUtils.encodeLong(bKeys, 9, value.getPredicate());
			context.write(this.key, new LongWritable(value.getSubject()));
		}

		if (schemaSymmetricProperties.containsKey(value.getPredicate())) {

			int schemaStep = schemaSymmetricProperties
					.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousDerivation)
				return;

			bKeys[0] = 2;
			NumberUtils.encodeLong(bKeys, 1, value.getSubject());
			NumberUtils.encodeLong(bKeys, 9, value.getObject());
			context.write(this.key, new LongWritable(value.getPredicate()));
		}

		if (schemaInverseOfProperties.containsKey(value.getPredicate())) {

			int schemaStep = schemaInverseOfProperties
					.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousDerivation)
				return;

			bKeys[0] = 3;
			NumberUtils.encodeLong(bKeys, 1, value.getSubject());
			NumberUtils.encodeLong(bKeys, 9, value.getObject());
			context.write(this.key, new LongWritable(value.getPredicate()));
		}
	}

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
