package partitioners;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class NaivePartitioner extends Partitioner<BytesWritable, NullWritable>
		implements Configurable {

	protected static Logger log = LoggerFactory
			.getLogger(NaivePartitioner.class);
	private Configuration c = null;

	// float partitionSize = 0;

	@Override
	public int getPartition(BytesWritable key, NullWritable value,
			int numPartitions) {
		// The task number is on the first three bytes, not 4.
		long resource = NumberUtils.decodeLong(key.getBytes(), 0);
		int number = (int) (resource >> 40);
		/*
		 * int partition = Math.round((number / partitionSize)); if (partition
		 * >= numPartitions) { log.error("Resource = " + resource + " number = "
		 * + number + " partition = " + partition); partition = numPartitions -
		 * 1; }
		 */
		return number % numPartitions;
	}

	@Override
	public Configuration getConf() {
		return c;
	}

	@Override
	public void setConf(Configuration conf) {
		this.c = conf;
		// Set upper limit
		/*
		 * long numReduceTasks = conf.getLong("numReduceTasks", 0) + 1; int
		 * numPartitions = conf.getInt("mapred.reduce.tasks", 0); partitionSize
		 * = (float) numReduceTasks / (float) numPartitions;
		 * log.debug("Partition size =" + partitionSize + " numReduceTasks = " +
		 * numReduceTasks + " numPartitions = " + numPartitions);
		 */
	}
}