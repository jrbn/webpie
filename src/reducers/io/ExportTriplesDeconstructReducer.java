package reducers.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.NumberUtils;

public class ExportTriplesDeconstructReducer extends
		Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	// private BytesWritable oKey = new BytesWritable();
	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	private java.util.List<byte[]> storage = new ArrayList<byte[]>();

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {

		storage.clear();
		if (key.get() < 0) { // Already converted entry. Output all values
			oKey.set((key.get() + 1) * -1);
			Iterator<BytesWritable> itr = values.iterator();
			while (itr.hasNext()) {
				BytesWritable value = itr.next();
				context.write(oKey, value);
			}
		} else { // Do the join in the naive way
			boolean dictionaryFounded = false;
			Iterator<BytesWritable> itr = values.iterator();
			while (itr.hasNext()) {
				BytesWritable value = itr.next();
				byte[] bValue = value.getBytes();
				if (bValue[0] == 0) { // It is the dictionary entry
					dictionaryFounded = true;
					// Copy the text in dictionary
					oValue.setSize(value.getLength());
					System.arraycopy(bValue, 1, oValue.getBytes(), 1,
							value.getLength() - 1);

					/*String term = new String(oValue.getBytes(), 0,
							oValue.getLength());
					//if (term.indexOf("Lecturer6") != -1) {
					if (term.equals("<http://www.Department9.University0.edu/Lecturer6>")) {
						System.out.println(term);
					}*/

					for (byte[] storageElement : storage) {
						oValue.getBytes()[0] = storageElement[0];
						oKey.set(NumberUtils.decodeLong(storageElement, 1));
						context.write(oKey, oValue);
					}
					storage.clear();
				} else {
					if (!dictionaryFounded) {
						byte[] element = new byte[9];
						System.arraycopy(bValue, 0, element, 0, 9);
						storage.add(element);
					} else {
						// Save as key the triple ID and as value the
						// position+value in text
						oKey.set(NumberUtils.decodeLong(bValue, 1));
						oValue.getBytes()[0] = bValue[0];
						context.write(oKey, oValue);
					}
				}
			}
			if (!dictionaryFounded)
				context.getCounter("error", "entries not found").increment(1);
		}
	}
}
