package reducers.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExportTriplesReconstructReducer extends
		Reducer<LongWritable, BytesWritable, NullWritable, Text> {

	Text oValue = new Text();

	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {

		String subject = null;
		String predicate = null;
		String object = null;

		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable value = itr.next();
			switch (value.getBytes()[0]) {
			case 1:
				subject = new String(value.getBytes(), 1, value.getLength() - 1);
				break;
			case 2:
				predicate = new String(value.getBytes(), 1,
						value.getLength() - 1);
				break;
			case 3:
				object = new String(value.getBytes(), 1, value.getLength() - 1);
				break;
			}
		}

		if (subject != null && predicate != null && object != null) {
			oValue.set(subject + " " + predicate + " " + object + " .");
			context.write(NullWritable.get(), oValue);
		}
	}

}
