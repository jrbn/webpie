package reducers.owl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWLAllSomeValuesReducer extends
		Reducer<BytesWritable, BytesWritable, TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLAllSomeValuesReducer.class);
	private Triple triple = new Triple();
	private TripleSource source = new TripleSource();

	private Collection<byte[]> types = new ArrayList<byte[]>();
	private Collection<byte[]> resources = new ArrayList<byte[]>();
	private int previousDerivation = -1;

	@Override
	public void reduce(BytesWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		types.clear();
		resources.clear();

		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable value = itr.next();
			byte[] bValue = value.getBytes();
			byte[] resource_step = new byte[12];
			System.arraycopy(bValue, 1, resource_step, 0, 12);
			if (bValue[0] == 1) { // Type triple
				types.add(resource_step);
			} else { // Resource triple
				resources.add(resource_step);
			}
		}

		if (types.size() > 0 && resources.size() > 0) {
			Iterator<byte[]> itrResource = resources.iterator();
			while (itrResource.hasNext()) {
				byte[] value = itrResource.next();
				long subject = NumberUtils.decodeLong(value, 0);
				int sstep = NumberUtils.decodeInt(value, 8);
				triple.setSubject(subject);
				Iterator<byte[]> itrTypes = types.iterator();
				while (itrTypes.hasNext()) {
					byte[] typeValue = itrTypes.next();
					long object = NumberUtils.decodeLong(typeValue, 0);
					int ostep = NumberUtils.decodeInt(typeValue, 8);
					triple.setObject(object);

					if (Math.max(sstep, ostep) >= (previousDerivation - 1)) // Not
																			// ideal
																			// but
																			// should
																			// work
						context.write(source, triple);
				}
			}
		}
	}

	@Override
	public void setup(Context context) {
		previousDerivation = context.getConfiguration().getInt(
				"reasoner.previousStep", -1);
		source.setDerivation(TripleSource.OWL_RULE_15);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));
		triple.setObjectLiteral(false);
		triple.setPredicate(TriplesUtils.RDF_TYPE);
	}
}
