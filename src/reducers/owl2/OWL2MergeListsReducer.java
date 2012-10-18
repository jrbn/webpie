package reducers.owl2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class OWL2MergeListsReducer extends
		Reducer<LongWritable, BytesWritable, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2MergeListsReducer.class);

	protected BytesWritable oKey = new BytesWritable();
	protected BytesWritable oValue = new BytesWritable();

	Set<byte[]> heads = new HashSet<byte[]>();
	Set<byte[]> tails = new HashSet<byte[]>();

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		heads.clear();
		tails.clear();

		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable value = itr.next();
			byte[] bValue = new byte[value.getLength() - 1];
			System.arraycopy(value.getBytes(), 1, bValue, 0,
					value.getLength() - 1);

			if (value.getBytes()[0] == 0) { // It is a head
				heads.add(bValue);
			} else { // Tail
				tails.add(bValue);
			}
		}

		Iterator<byte[]> itrHeads = heads.iterator();
		while (itrHeads.hasNext()) {
			byte[] bValue = itrHeads.next();
			if (tails.size() > 0) {
				context.getCounter("reasoner", "merge").increment(tails.size());
				Iterator<byte[]> itrTails = tails.iterator();
				while (itrTails.hasNext()) {
					byte[] bValueTail = itrTails.next();
					NumberUtils.encodeLong(oKey.getBytes(), 0,
							NumberUtils.decodeLong(bValueTail, 0));
					NumberUtils.encodeLong(oKey.getBytes(), 8,
							NumberUtils.decodeLong(bValue, 0));
					oValue.setSize(bValue.length - 8 + bValueTail.length - 8);
					System.arraycopy(bValueTail, 8, oValue.getBytes(), 0,
							bValueTail.length - 8);
					System.arraycopy(bValue, 8, oValue.getBytes(),
							bValueTail.length - 8, bValue.length - 8);
					context.write(oKey, oValue);
				}
			} else {
				NumberUtils.encodeLong(oKey.getBytes(), 0, key.get());
				NumberUtils.encodeLong(oKey.getBytes(), 8,
						NumberUtils.decodeLong(bValue, 0));
				oValue.setSize(bValue.length - 8);
				System.arraycopy(bValue, 8, oValue.getBytes(), 0,
						bValue.length - 8);
				context.write(oKey, oValue);
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		oKey.setSize(16);
	}
}
