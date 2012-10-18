package partitioners;

import java.util.ArrayList;
import java.util.Arrays;

import jobs.CreateIndex;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BalancedPartitioner extends
	HashPartitioner<BytesWritable, NullWritable> implements Configurable {

    protected static Logger log = LoggerFactory
	    .getLogger(BalancedPartitioner.class);
    Configuration c;

    static byte[][][] partitions = null;

    @Override
    public int getPartition(BytesWritable key, NullWritable value,
	    int numPartitions) {

	int i = 0;
	while (i < partitions[key.getBytes()[0]].length
		&& WritableComparator.compareBytes(key.getBytes(), 1, 24,
			partitions[key.getBytes()[0]][i], 0, 24) >= 0) {
	    ++i;
	}

	return i;
    }

    @Override
    public Configuration getConf() {
	return c;
    }

    @Override
    public void setConf(Configuration c) {
	this.c = c;
	if (partitions == null) {
	    try {
		partitions = new byte[CreateIndex.indices.length][][];		
		Path location = new Path(c.get("partitionsLocation"));

		int i = 0;
		for (String index : CreateIndex.indices) {
			ArrayList<byte[]> listPartitions = new ArrayList<byte[]>(
					c.getInt("mapred.reduce.tasks", 0) - 1);
		    FileSystem fs = location.getFileSystem(c);
		    FileStatus[] files = fs.listStatus(new Path(location, index
			    + "/_partitions"), new OutputLogFilter());
		    BytesWritable key = new BytesWritable();
		    NullWritable value = NullWritable.get();
		    for (FileStatus file : files) {
			if (!file.getPath().getName().startsWith("_")) {
			    // Add it to the list
			    SequenceFile.Reader input = null;
			    try {
				input = new SequenceFile.Reader(fs,
					file.getPath(), c);
				boolean nextValue = false;
				do {
				    nextValue = input.next(key, value);
				    if (nextValue) {
					byte[] pb = Arrays
						.copyOf(key.getBytes(),
							key.getLength());
					listPartitions.add(pb);
				    }
				} while (nextValue);

			    } finally {
				if (input != null) {
				    input.close();
				}
			    }
			}
		    }

		    partitions[i] = new byte[listPartitions.size()][];
		    listPartitions.toArray(partitions[i]);
		    log.info("Partition " + i + " size=" + partitions[i].length);
		    i++;
		}

	    } catch (Exception e) {
		log.error("Could not load the partition table", e);
	    }
	} else {
	    log.debug("partition table already loaded!");
	}
    }
}