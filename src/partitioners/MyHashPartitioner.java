package partitioners;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyHashPartitioner<K,V> extends HashPartitioner<K, V> {
	
	@Override
	public int getPartition(K key, V value, int numPartitions) {
		int partition = Math.abs(key.toString().hashCode() % numPartitions);
		return partition;
	}
}