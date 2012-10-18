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

public class OWL2JoinFirstRestReducer extends
		Reducer<LongWritable, BytesWritable, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2JoinFirstRestReducer.class);
	
	protected BytesWritable oKey = new BytesWritable();
	protected BytesWritable oValue = new BytesWritable();

	Set<Long> rests = new HashSet<Long>();
	
	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		rests.clear();
		long first = -1;
		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable value = itr.next();
			if (value.getBytes()[0] == 0) { //First
				first = NumberUtils.decodeLong(value.getBytes(), 1);				
			} else {
				long resource = NumberUtils.decodeLong(value.getBytes(), 1);
				rests.add(resource);
			}
		}
		
		if (first != -1 && rests.size() > 0) {
			NumberUtils.encodeLong(oKey.getBytes(), 0, key.get());
			NumberUtils.encodeLong(oValue.getBytes(), 0, first);
			
			Iterator<Long> itr2 = rests.iterator();
			while (itr2.hasNext()) {
				long resource = itr2.next();
				NumberUtils.encodeLong(oKey.getBytes(), 8, resource);
				context.write(oKey, oValue);
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		oKey.setSize(16);
		oValue.setSize(8);
	}
}
