package partitioners;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class BalancedPartitioner2 extends
		HashPartitioner<BytesWritable, NullWritable> {

	@Override
	public int getPartition(BytesWritable key, NullWritable value,
			int numReduceTasks) {
		// Input is an array of 24 bytes
		int hash = WritableComparator
				.hashBytes(key.getBytes(), key.getLength());
		return (hash & Integer.MAX_VALUE) % numReduceTasks;
	}
}