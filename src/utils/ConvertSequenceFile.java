package utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

public class ConvertSequenceFile {

	public static void main(String[] args) throws IOException {
		// Read the sequence file and print in text

		FileWriter writer = new FileWriter(new File(args[0] + ".txt"));
		
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			LongWritable key = new LongWritable();
			BytesWritable value = new BytesWritable();
			SequenceFile.Reader input = new SequenceFile.Reader(fs, fs.getFileStatus(new Path(args[0]))
					.getPath(), new Configuration());
			
			boolean nextValue = false;
			do {
				nextValue = input.next(key, value);
				if (nextValue) {
					writer.write(key + "\t" + new String(value.getBytes(), 0, value.getLength()) + "\n");
				}
			} while (nextValue);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		writer.flush();
		writer.close();

	}

}
