package reducers.owl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.NumberUtils;
import data.Triple;
import data.TripleSource;

public class OWLSameAsReconstructReducer extends
		Reducer<BytesWritable, BytesWritable, TripleSource, Triple> {

	private TripleSource oKey = new TripleSource();
	private Triple oValue = new Triple();

	@Override
	public void reduce(BytesWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		byte[] bKey = key.getBytes();
		oKey.setStep(NumberUtils.decodeInt(bKey, 8));
		oKey.setDerivation(bKey[12]);

		int elements = 0;
		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			elements++;
			byte[] bValue = itr.next().getBytes();
			long resource = NumberUtils.decodeLong(bValue, 1);
			switch (bValue[0]) {
			case 0:
				oValue.setSubject(resource);
				break;
			case 1:
				oValue.setPredicate(resource);
				break;
			case 2:
			case 3:
				if (bValue[0] == 2)
					oValue.setObjectLiteral(false);
				else
					oValue.setObjectLiteral(true);
				oValue.setObject(resource);
				break;
			default:
				break;
			}
		}

		if (elements == 3)
			context.write(oKey, oValue);
	}
}
