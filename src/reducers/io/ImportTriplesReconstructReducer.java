package reducers.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.Triple;
import data.TripleSource;

public class ImportTriplesReconstructReducer extends
		Reducer<LongWritable, LongWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(ImportTriplesReconstructReducer.class);

	private Triple oValue = new Triple();
	private TripleSource oKey = new TripleSource();

	public void reduce(LongWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		Iterator<LongWritable> itr = values.iterator();
		int counter = 0;
		while (itr.hasNext()) {
			counter++;

			// Get the position
			long value = itr.next().get();
			long pos = value & 0x3;
			if (pos == 0) {
				oValue.setSubject(value >> 2);
			} else if (pos == 1) {
				oValue.setPredicate(value >> 2);
			} else if (pos == 2) {
				oValue.setObject(value >> 2);
				oValue.setObjectLiteral(false);
			} else if (pos == 3) {
				oValue.setObject(value >> 2);
				oValue.setObjectLiteral(true);
			} else {
				// throw new IOException("Position not clear!!");
				log.error("Position not clear!!");
			}
		}

		if (counter != 3) {
			// throw new IOException("Triple is not reconstructed!");
			log.error("Triple is not reconstructed!" + key.get());
		}

		context.getCounter("output", "records").increment(1);
		context.write(oKey, oValue);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		oKey.setStep(context.getConfiguration().getInt("inputStep", 0));
		log.info("Set step to " + oKey.getStep());
	}
}
