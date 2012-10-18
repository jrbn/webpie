package reducers.owl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.NumberUtils;

public class OWLSameAsDeconstructReducer extends Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	
	private List<byte[]> storage = new LinkedList<byte[]>();
	
	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {		

		storage.clear();
		long countOutput = 0;
		oKey.set(key.get());
		boolean replacement = false;
		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable iValue = itr.next();
			byte[] bValue = iValue.getBytes();
			if (bValue[0] == 4) {//Same as
				long resource = NumberUtils.decodeLong(bValue, 1);
				replacement = true;
				oKey.set(resource);
			} else if (!replacement && bValue[14] == 0) {
				//Store in memory the results
				byte[] newValue = Arrays.copyOf(bValue, bValue.length);
				storage.add(newValue);
			} else {
				//I already found the replacement. Simply output everything
				context.write(oKey, iValue);
				countOutput++;
				context.getCounter("reasoner", "substitutions").increment(1);
			}
		}
		
		Iterator<byte[]> itr2 = storage.iterator();
		while (itr2.hasNext()) {
			byte[] bValue = itr2.next();
			oValue.set(bValue, 0, bValue.length);
			context.write(oKey, oValue);
		}
		
		if (replacement) { //Increment counter of replacements
			context.getCounter("reasoner", "substitutions").increment(countOutput + storage.size());
		}
	}
}
