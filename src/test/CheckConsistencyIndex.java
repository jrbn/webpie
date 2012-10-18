package test;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.OutputLogFilter;

public class CheckConsistencyIndex {

	public static void main(String[] args) throws IOException {
		if (args == null || args.length < 1) {
			return;
		}

		System.out.println("Checking the consistency of the index at "
				+ args[0]);

		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] files = fs.listStatus(new Path(args[0]),
				new OutputLogFilter());
		byte[] previous = null;

		BytesWritable key = new BytesWritable();
		for (FileStatus file : files) {
			SequenceFile.Reader input = null;
			input = new SequenceFile.Reader(fs, file.getPath(),
					new Configuration());
			boolean nextValue = false;
			do {
				nextValue = input.next(key, NullWritable.get());
				if (nextValue) {
					if (previous != null) {
						if (BytesWritable.Comparator.compareBytes(previous, 0,
								previous.length, key.getBytes(), 0,
								key.getLength()) > 0) {
							System.err.println("Index NOT consistent");
							return;
						}
					}
					previous = Arrays.copyOf(key.getBytes(), key.getLength());
				}
			} while (nextValue);
			input.close();
		}

		System.out.println("index consistent!!!");
	}
}
