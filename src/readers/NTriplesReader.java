package readers;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NTriplesReader extends MultiFilesReader<Text, Text> {
	
	public class NTriplesRecordReader extends RecordReader<Text, Text> {
		MultiFilesSplit split = null;
		TaskAttemptContext context = null;
		
		private LineRecordReader rr = new LineRecordReader();
		private Text fileName = new Text();
		int i = 0;
		
		@Override
		public synchronized void close() throws IOException {
			rr.close();
		}

		@Override
		public Text getCurrentKey() throws IOException,
				InterruptedException {
			return fileName;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return rr.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float)i / (float)split.getFiles().size();
		}
		
		private void openNextFile() throws IOException {
			//Close current record reader
			rr.close();
			
			if (i < split.getFiles().size()) {
				FileStatus currentFile = split.getFiles().get(i);
				FileSplit fSplit = new FileSplit(currentFile.getPath(), 0, currentFile.getLen(), null);
				rr.initialize(fSplit, context);
				fileName.set(currentFile.getPath().getName());
				++i;
			}
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.split = (MultiFilesSplit)split;
			this.context = context;
			i = 0;
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
	
	protected static Logger log = LoggerFactory.getLogger(NTriplesReader.class);
	
	@Override
	public RecordReader<Text, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new NTriplesRecordReader();
	}
}
