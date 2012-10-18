package reducers.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckConsistencyCompressionReducer extends Reducer<Text, NullWritable, LongWritable, Text> {

	protected static Logger log = LoggerFactory.getLogger(CheckConsistencyCompressionReducer.class);
	
	public void reduce(Text key, Iterable<NullWritable> values,  Context context) throws IOException, InterruptedException { 
		int count = 0;
		Iterator<NullWritable> itr = values.iterator();
		
		while (itr.hasNext()) {
			count++;
			itr.next();
		}
		
		if (count != 2) {
			context.write(new LongWritable(count), key);
			context.setStatus("not consistent");
		}
	}
}
