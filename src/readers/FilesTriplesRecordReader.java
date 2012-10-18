package readers;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import data.Triple;
import data.TripleSource;

public class FilesTriplesRecordReader extends
		RecordReader<TripleSource, Triple> {
	MultiFilesSplit split = null;
	TaskAttemptContext context = null;

	private SequenceFileRecordReader<TripleSource, Triple> rr = new SequenceFileRecordReader<TripleSource, Triple>();
	int i = 0;

	@Override
	public synchronized void close() throws IOException {
		rr.close();
	}

	@Override
	public TripleSource getCurrentKey() throws IOException,
			InterruptedException {
		return rr.getCurrentKey();
	}

	@Override
	public Triple getCurrentValue() throws IOException, InterruptedException {
		return rr.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) i / (float) split.getFiles().size();
	}

	private void openNextFile() throws IOException, InterruptedException {
		// Close current record reader
		if (i > 0)
			rr.close();

		if (i < split.getFiles().size()) {
			FileStatus currentFile = split.getFiles().get(i);
			FileSplit fSplit = new FileSplit(currentFile.getPath(), 0,
					currentFile.getLen(), null);
			rr.initialize(fSplit, context);
			++i;
		}
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.split = (MultiFilesSplit) split;
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
