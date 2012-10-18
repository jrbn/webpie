package readers;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

public class NTriplesCombinedReader extends
		MultiFilesReader<BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(NTriplesCombinedReader.class);

	public class NTriplesCombinedRecordReader extends
			RecordReader<BytesWritable, BytesWritable> {
		MultiFilesSplit split = null;
		TaskAttemptContext context = null;

		boolean isTripleFile = true;
		private LineRecordReader rr = new LineRecordReader();
		private SequenceFileRecordReader<LongWritable, BytesWritable> dr = new SequenceFileRecordReader<LongWritable, BytesWritable>();
		private String fileName = null;

		private BytesWritable oKey = new BytesWritable();
		private BytesWritable oValue = new BytesWritable();

		int i = 0;

		@Override
		public synchronized void close() throws IOException {
			if (isTripleFile)
				rr.close();
			else
				dr.close();
		}

		@Override
		public BytesWritable getCurrentKey() throws IOException,
				InterruptedException {
			if (isTripleFile) {
				byte[] name = fileName.getBytes();
				oKey.setSize(name.length + 1);
				oKey.getBytes()[0] = 0;
				System.arraycopy(name, 0, oKey.getBytes(), 1, name.length);
			} else {
				oKey.setSize(9);
				oKey.getBytes()[0] = 1;
				NumberUtils.encodeLong(oKey.getBytes(), 1, dr.getCurrentKey()
						.get());
			}

			return oKey;
		}

		@Override
		public BytesWritable getCurrentValue() throws IOException,
				InterruptedException {
			if (isTripleFile) {
				byte[] line = rr.getCurrentValue().toString().getBytes();
				oValue.setSize(line.length);
				System.arraycopy(line, 0, oValue.getBytes(), 0, line.length);
				return oValue;
			} else {
				return dr.getCurrentValue();
			}
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float) i / (float) split.getFiles().size();
		}

		private boolean openNextFile() throws IOException, InterruptedException {
			// Close current record reader
			if (isTripleFile)
				rr.close();
			else
				dr.close();

			if (i < split.getFiles().size()) {
				FileStatus currentFile = split.getFiles().get(i);
				FileSplit fSplit = new FileSplit(currentFile.getPath(), 0,
						currentFile.getLen(), null);
				if (currentFile.getPath().getName().endsWith(".gz")) {
					rr.initialize(fSplit, context);
					isTripleFile = true;
					fileName = currentFile.getPath().getName();
				} else {
					dr.initialize(fSplit, context);
					isTripleFile = false;
				}

				++i;
				return true;
			}

			return false;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.split = (MultiFilesSplit) split;
			this.context = context;
			i = 0;
			openNextFile();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean value = false;
			if (isTripleFile) {
				try {
					value = rr.nextKeyValue();
				} catch (Exception e) {
					log.error("Error in parsing the file: "
							+ split.getFiles().get(i - 1).getPath(), e);
					value = false;
				}
			} else {
				value = dr.nextKeyValue();
			}

			if (!value) {
				if (openNextFile())
					return nextKeyValue();
				else
					return false;
			} else {
				return true;
			}

		}

	}

	@Override
	public RecordReader<BytesWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new NTriplesCombinedRecordReader();
	}

}
