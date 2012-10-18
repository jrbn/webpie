package partitioners;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import utils.HashFunctions;

//import utils.HashFunctions;

public class TriplesPartitioner<K, V> extends HashPartitioner<K, V> {

	@Override
	public int getPartition(K key, V value, int numPartitions) {
		int partition = Math.abs(Math.abs(HashFunctions.JSHashInt(key
				.toString())) % numPartitions);
		return partition;
		
//		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}