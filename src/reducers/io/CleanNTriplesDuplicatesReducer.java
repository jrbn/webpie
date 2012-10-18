package reducers.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanNTriplesDuplicatesReducer extends
		Reducer<Text, NullWritable, NullWritable, Text> {

	public void reduce(Text key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		Iterator<NullWritable> itr = values.iterator();
		while (itr.hasNext() && count < 10) {
			itr.next();
			count++;
		}
		context.write(NullWritable.get(), key);
	}
}
