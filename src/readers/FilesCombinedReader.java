package readers;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import utils.NumberUtils;
import data.Triple;
import data.TripleSource;

public class FilesCombinedReader extends
		MultiFilesReader<BytesWritable, BytesWritable> {

	@Override
	public RecordReader<BytesWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new FilesCombinedRecordReader();
	}

	public class FilesCombinedRecordReader extends
			RecordReader<BytesWritable, BytesWritable> {

		MultiFilesSplit split = null;
		TaskAttemptContext context = null;
		int nextFile = 0;
		boolean isDictionary = false;

		SequenceFileRecordReader<LongWritable, BytesWritable> dr = new SequenceFileRecordReader<LongWritable, BytesWritable>();
		SequenceFileRecordReader<TripleSource, Triple> tr = new SequenceFileRecordReader<TripleSource, Triple>();

		BytesWritable oKey = new BytesWritable();
		BytesWritable oValue = new BytesWritable();

		@Override
		public void close() throws IOException {
			if (isDictionary) {
				dr.close();
			} else {
				tr.close();
			}
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.split = (MultiFilesSplit) split;
			this.context = context;
			nextFile = 0;
			openNextFile();
		}

		@Override
		public BytesWritable getCurrentKey() throws IOException,
				InterruptedException {
			if (isDictionary) {
				oKey.setSize(9);
				oKey.getBytes()[0] = 1;
				NumberUtils.encodeLong(oKey.getBytes(), 1, dr.getCurrentKey()
						.get());
			} else {
				oKey.setSize(1);
				oKey.getBytes()[0] = 0;
			}

			return oKey;
		}

		@Override
		public BytesWritable getCurrentValue() throws IOException,
				InterruptedException {
			if (isDictionary) {
				return dr.getCurrentValue();
			} else {
				Triple triple = tr.getCurrentValue();
				oValue.setSize(24);
				NumberUtils.encodeLong(oValue.getBytes(), 0,
						triple.getSubject());
				NumberUtils.encodeLong(oValue.getBytes(), 8,
						triple.getPredicate());
				NumberUtils.encodeLong(oValue.getBytes(), 16,
						triple.getObject());
				return oValue;
			}
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float) (nextFile) / (float) split.getLength();
		}

		private void openNextFile() throws IOException, InterruptedException {
			if (nextFile > 0) {
				if (isDictionary) {
					dr.close();
				} else {
					tr.close();
				}

			}

			if (nextFile < split.getFiles().size()) {
				FileStatus currentFile = split.getFiles().get(nextFile);
				FileSplit fSplit = new FileSplit(currentFile.getPath(),
						split.getStart(nextFile), split.getEnds(nextFile)
								- split.getStart(nextFile), null);

				if (currentFile.getPath().toString().indexOf("_dict") == -1) { // Triple
					tr.initialize(fSplit, context);
					isDictionary = false;
				} else {
					dr.initialize(fSplit, context);
					isDictionary = true;
				}

				++nextFile;
			}
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean value = false;
			if (isDictionary) {
				value = dr.nextKeyValue();
			} else {
				value = tr.nextKeyValue();
			}

			while (!value && nextFile < split.getFiles().size()) {
				openNextFile();
				if (isDictionary) {
					value = dr.nextKeyValue();
				} else {
					value = tr.nextKeyValue();
				}
			}

			return value;
		}
	}
}
