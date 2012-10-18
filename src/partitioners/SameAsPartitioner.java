package partitioners;

import java.util.Random;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

public class SameAsPartitioner extends MyHashPartitioner<LongWritable, BytesWritable> {
	
	private Random random = new Random();
	
	@Override
	public int getPartition(LongWritable key, BytesWritable value, int numPartitions) {
		byte[] bValue = value.getBytes();
		if (bValue[14] == 1) {
			return random.nextInt(numPartitions);
		}
		
		return super.getPartition(key, value, numPartitions);
	}
}