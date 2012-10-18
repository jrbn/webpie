package mappers.io;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import utils.HashFunctions;
import data.Triple;
import data.TripleSource;

public class CheckTripleDistributionMapper extends
		Mapper<TripleSource, Triple, IntWritable, IntWritable> {

	private IntWritable oValue = new IntWritable(1);
	private IntWritable oKey = new IntWritable();
	private int partitions = 0;

	protected void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		/*
		 * long number = value.getSubject() & Integer.MAX_VALUE +
		 * value.getPredicate() & Integer.MAX_VALUE + value.getObject() &
		 * Integer.MAX_VALUE;
		 */

		int partition = Math.abs((int) HashFunctions
				.JSHash("1225130573719142656 728 1153147729124261888"))
				% partitions;
		oKey.set(partition);
		context.write(oKey, oValue);

		// oKey.set(Math.abs((int)HashFunctions.JSHashInt(value.toString())) %
		// partitions);
		// context.write(oKey, oValue);

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		partitions = context.getConfiguration().getInt("partitions", 0);
	}
}