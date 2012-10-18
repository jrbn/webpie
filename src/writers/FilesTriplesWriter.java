package writers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class FilesTriplesWriter extends
		SequenceFileOutputFormat<TripleSource, Triple> {

	public RecordWriter<TripleSource, Triple> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new TriplesRecordWriter(context);
	}

	public class TriplesRecordWriter extends RecordWriter<TripleSource, Triple> {

		TaskAttemptContext context = null;
		Path workingDir = null;
		Map<Integer, SequenceFile.Writer> files = new HashMap<Integer, SequenceFile.Writer>();

		public TriplesRecordWriter(TaskAttemptContext context) {
			this.context = context;
			try {
				FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
				workingDir = committer.getWorkPath();
			} catch (Exception e) {				
			}
		}
		
		public TriplesRecordWriter(TaskAttemptContext context, Path workingDir) {
			this.context = context;
			this.workingDir = workingDir;
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			Iterator<SequenceFile.Writer> itr = files.values().iterator();
			while (itr.hasNext()) {
				itr.next().close();
			}
		}

		@Override
		public void write(TripleSource key, Triple value) throws IOException,
				InterruptedException {
			int type = TriplesUtils.getTripleType(key, value.getSubject(),
					value.getPredicate(), value.getObject());
			SequenceFile.Writer writer = files.get(type);
			if (writer == null) { // Create new file
				String name = TriplesUtils.getFileExtensionByTripleType(key,
						value, getUniqueFile(context, "triples", ""));
				Path path = new Path(workingDir, name);
				writer = SequenceFile.createWriter(
						FileSystem.get(context.getConfiguration()),
						context.getConfiguration(), path, TripleSource.class,
						Triple.class, CompressionType.BLOCK,
						new DefaultCodec(), context);
				files.put(type, writer);
			}

			writer.append(key, value);
		}
	}
}