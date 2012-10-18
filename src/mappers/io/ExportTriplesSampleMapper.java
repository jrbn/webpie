package mappers.io;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import data.Triple;
import data.TripleSource;

public class ExportTriplesSampleMapper extends Mapper<TripleSource, Triple, LongWritable, NullWritable>  {
	
	private LongWritable oKey = new LongWritable();
	private Random random = new Random();
	private int threshold = 0;
	
	@Override
	 protected void map(TripleSource key, Triple value, Context context) {
		try {
			int randomNumber = random.nextInt(100);
			if (randomNumber < threshold) {
				oKey.set(value.getSubject());
				context.write(oKey, NullWritable.get());
				oKey.set(value.getPredicate());
				context.write(oKey, NullWritable.get());
				oKey.set(value.getObject());
				context.write(oKey, NullWritable.get());
			}
		} catch (Exception e) {}
	}
	
	protected void setup(Context context) throws IOException, InterruptedException {
		threshold = context.getConfiguration().getInt("reasoner.samplingPercentage", 0);
	}
}