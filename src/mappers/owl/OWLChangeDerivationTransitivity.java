package mappers.owl;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.Triple;
import data.TripleSource;

public class OWLChangeDerivationTransitivity extends
		Mapper<TripleSource, Triple, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLChangeDerivationTransitivity.class);

	private int currentExecution = -1;

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		if (key.getStep() > currentExecution) {
			key.setAlreadyFiltered(true);
			key.setStep(currentExecution);
			context.write(key, value);
		}
	}

	protected void setup(Context context) throws IOException {
		currentExecution = context.getConfiguration().getInt(
				"reasoner.currentExecution", 0);
	}
}
