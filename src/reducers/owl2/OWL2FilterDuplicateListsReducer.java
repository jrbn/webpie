package reducers.owl2;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class OWL2FilterDuplicateListsReducer extends
		Reducer<LongWritable, BytesWritable, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2FilterDuplicateListsReducer.class);

	protected BytesWritable oKey = new BytesWritable();
	protected BytesWritable oValue = new BytesWritable();

	List<byte[]> lists = new LinkedList<byte[]>();

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		lists.clear();

		Iterator<BytesWritable> itr = values.iterator();
		boolean different = false;
		while (itr.hasNext() && !different) {
			BytesWritable value = itr.next();
			// Scroll the list to see whether there is already a longer list
			ListIterator<byte[]> itr2 = lists.listIterator();

			while (itr2.hasNext() && !different) {
				byte[] listValues = itr2.next();
				for (int i = 8; i < listValues.length; i += 8) {
					if (value.getLength() < (i + 8)) {
						different = true;
					} else {
						long listValue = NumberUtils.decodeLong(listValues, i);
						long lValue = NumberUtils.decodeLong(value.getBytes(),
								i);
						if (listValue != lValue) {
							different = true;
						}
					}
				}

				if (!different) {
					itr2.remove();
					break;
				}
			}

			if (!different) {
				byte[] newValue = new byte[value.getLength()];
				System.arraycopy(value.getBytes(), 0, newValue, 0,
						value.getLength());
				lists.add(newValue);
			}
		}

		// Output all the values in the list
		NumberUtils.encodeLong(oKey.getBytes(), 0, key.get());
		Iterator<byte[]> itr3 = lists.iterator();
		while (itr3.hasNext()) {
			byte[] value = itr3.next();
			NumberUtils.encodeLong(oKey.getBytes(), 8,
					NumberUtils.decodeLong(value, 0));
			oValue.setSize(value.length - 8);
			System.arraycopy(value, 8, oValue.getBytes(), 0, value.length - 8);
			context.write(oKey, oValue);
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		oKey.setSize(16);
	}
}
