package writers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import data.Triple;
import data.TripleSource;

public class SplitOutput extends FilesTriplesWriter {

	public RecordWriter<TripleSource, Triple> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new SplitRecordWriter(context);
	}

	public class SplitRecordWriter extends RecordWriter<TripleSource, Triple> {

		Map<Integer, TriplesRecordWriter> splits = new java.util.HashMap<Integer, TriplesRecordWriter>();
		TaskAttemptContext context = null;

		public SplitRecordWriter(TaskAttemptContext context) {
			this.context = context;
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {
			for (TriplesRecordWriter split : splits.values()) {
				split.close(context);
			}
		}

		@Override
		public void write(TripleSource key, Triple value) throws IOException,
				InterruptedException {
			int split = key.getStep();
			TriplesRecordWriter splitWriter = splits.get(split);
			if (splitWriter == null) {
				// Create splitWriter and store it
				FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
				Path workingDir = committer.getWorkPath();
				splitWriter = new FilesTriplesWriter.TriplesRecordWriter(
						context, new Path(workingDir, Integer.toString(split)));
				splits.put(split, splitWriter);
			}
			splitWriter.write(key, value);
		}

	}

}
