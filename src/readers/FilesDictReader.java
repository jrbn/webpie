package readers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

public class FilesDictReader extends
		MultiFilesReader<LongWritable, BytesWritable> {

	public static Map<String, Long> readCommonResources(Configuration context,
			Path dir) {
		HashMap<String, Long> map = new HashMap<String, Long>();

		try {
			FileSystem fs = dir.getFileSystem(context);
			FileStatus oldFiles[] = fs.listStatus(new Path(dir, "old"), new OutputLogFilter());
			FileStatus newFiles[] = fs.listStatus(new Path(dir, "new"), new OutputLogFilter());
			FileStatus[] files = null;
			if (newFiles != null && oldFiles != null) {
				files = new FileStatus[newFiles.length + oldFiles.length];
				System.arraycopy(oldFiles, 0, files, 0, oldFiles.length);
				System.arraycopy(newFiles, 0, files, oldFiles.length,
						newFiles.length);
			} else {
				if (newFiles != null)
					files = newFiles;
				else if (oldFiles != null)
					files = oldFiles;					
			}

			if (files != null) {
				LongWritable key = new LongWritable();
				BytesWritable value = new BytesWritable();
				for (FileStatus file : files) {
					SequenceFile.Reader input = new SequenceFile.Reader(fs,
							file.getPath(), context);
					boolean nextValue = false;
					do {
						nextValue = input.next(key, value);
						if (nextValue)
							map.put(new String(value.getBytes(), 0, value
									.getLength()), key.get());
					} while (nextValue);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return map;
	}

	public class DictRecordReader extends RecordReader<LongWritable, BytesWritable> {
		MultiFilesSplit split = null;
		TaskAttemptContext context = null;
		
		private SequenceFileRecordReader<LongWritable, BytesWritable> rr = 
			new SequenceFileRecordReader<LongWritable, BytesWritable>(); 
		int i = 0;
		
		@Override
		public synchronized void close() throws IOException {
			rr.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return rr.getCurrentKey();
		}

		@Override
		public BytesWritable getCurrentValue() throws IOException, InterruptedException {
			return rr.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float)i / (float)split.getFiles().size();
		}
		
		private void openNextFile() throws IOException, InterruptedException {
			//Close current record reader		
			if (i > 0)
				rr.close();
			
			if (i < split.getFiles().size()) {
				FileStatus currentFile = split.getFiles().get(i);
				FileSplit fSplit = new FileSplit(currentFile.getPath(), split.getStart(i), split.getEnds(i) - split.getStart(i), null);
				rr.initialize(fSplit, context);
				++i;
			}
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.split = (MultiFilesSplit)split;
			this.context = context;
			openNextFile();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean value = false;
			while (!(value = rr.nextKeyValue()) && i < split.getFiles().size()) {
				openNextFile();
			}
			
			return value;
		}
	}
	
	@Override
	public RecordReader<LongWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new DictRecordReader();
	}
	
	public static Map<Long, String> readInvertedCommonResources(Configuration context, Path dir) {
		HashMap<Long, String> map = new HashMap<Long, String>();
		
		try {
			FileSystem fs = dir.getFileSystem(context);
			FileStatus files[] =  fs.listStatus(dir, new OutputLogFilter());
			if (files != null) {
				LongWritable key = new LongWritable();
				BytesWritable value = new BytesWritable();				
				for(FileStatus file : files) {
					SequenceFile.Reader input = new SequenceFile.Reader(fs, file.getPath(), context);
					boolean nextValue = false;
					do {
						nextValue = input.next(key, value);
						if (nextValue) map.put(key.get(), new String(value.getBytes(), 0, value.getLength()));
					} while (nextValue);
				}
			}
		} catch (Exception e) { 
			e.printStackTrace();
		}
		
		return map;
	}
	
	public static Set<Long> readSetCommonResources(Configuration context, Path dir) {
		Set<Long> map = new HashSet<Long>();
		
		try {
			FileSystem fs = dir.getFileSystem(context);
			FileStatus files[] =  fs.listStatus(dir, new OutputLogFilter());
			if (files != null) {
				LongWritable key = new LongWritable();
				BytesWritable value = new BytesWritable();				
				for(FileStatus file : files) {
					SequenceFile.Reader input = new SequenceFile.Reader(fs, file.getPath(), context);
					boolean nextValue = false;
					do {
						nextValue = input.next(key, value);
						if (nextValue) map.add(key.get());
					} while (nextValue);
				}
			}
		} catch (Exception e) { 
			e.printStackTrace();
		}
		
		return map;
	}
}
