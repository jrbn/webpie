package readers;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import utils.NumberUtils;

public class ConvertFilesReader extends MultiFilesReader<BytesWritable, Text> {

	public class ConvertFilesRecordReader extends RecordReader<BytesWritable, Text> {
		MultiFilesSplit split = null;
		TaskAttemptContext context = null;
		int i = -1;
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.split = (MultiFilesSplit)split;
			this.context = context;
			i = -1;
		}

		@Override
		public void close() throws IOException { }

		@Override
		public BytesWritable getCurrentKey() throws IOException,
				InterruptedException {
			BytesWritable key = new BytesWritable();
			key.setSize(16);
			NumberUtils.encodeLong(key.getBytes(), 0, split.getStart(i));
			NumberUtils.encodeLong(key.getBytes(), 8, split.getEnds(i));
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return new Text(split.getFiles().get(i).getPath().toString());
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {			
			return (float)i/(float)split.getFiles().size();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (++i<split.getFiles().size()) {
				return true;
			} else {
				return false;
			}
		}
	}
	
	@Override
	public RecordReader<BytesWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new ConvertFilesRecordReader();
	}

}
