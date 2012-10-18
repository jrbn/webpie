package mappers.owl;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;

import data.Triple;
import data.TripleSource;

public class OWLExtractTransitivityTriples extends
		Mapper<TripleSource, Triple, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLExtractTransitivityTriples.class);

	protected static Map<Long, Integer> schemaTransitiveProperties = null;
	private int previousExecution = -1;
	private int currentExecution = -1;

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		if (schemaTransitiveProperties.containsKey(value.getPredicate())) {

			int schemaStep = schemaTransitiveProperties.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousExecution) {
				key.setTransitivityActive(false);
			} else {
				key.setTransitivityActive(true);
				context.getCounter("OWL derived triples",
						"new transitivity triples").increment(1);
			}
			key.setStep(currentExecution);

			context.write(key, value);
		}
	}

	protected void setup(Context context) throws IOException {
		previousExecution = context.getConfiguration().getInt(
				"reasoner.previousExecution", -1);
		currentExecution = context.getConfiguration().getInt(
				"reasoner.currentExecution", 0);

		if (schemaTransitiveProperties == null) {
			schemaTransitiveProperties = FilesTriplesReader
					.loadTriplesWithStep("FILTER_ONLY_OWL_TRANSITIVE_SCHEMA",
							context);
		}
	}
}
