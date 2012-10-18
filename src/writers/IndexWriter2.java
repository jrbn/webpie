package writers;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.zip.GZIPOutputStream;

import jobs.CreateIndex;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

public class IndexWriter2 extends FileOutputFormat<BytesWritable, NullWritable> {

    @Override
    public void checkOutputSpecs(JobContext job)
	    throws FileAlreadyExistsException, IOException {
	// Ensure that the output directory is set and not already there
	Path outDir = getOutputPath(job);
	if (outDir == null) {
	    throw new InvalidJobConfException("Output directory not set.");
	}

	if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
	    Log.warn("Output directory " + outDir
		    + " already exists but I go on anyway");
	}
    }

    private class IndexRecordWriter extends
	    RecordWriter<BytesWritable, NullWritable> {

	int limit = 0;
	TaskAttemptContext context = null;
	GZIPOutputStream[] fouts = new GZIPOutputStream[CreateIndex.indices.length];
	int[] counts = new int[CreateIndex.indices.length];
	int[] chunks = new int[CreateIndex.indices.length];

	public IndexRecordWriter(TaskAttemptContext context) throws IOException {
	    this.context = context;
	    limit = context.getConfiguration().getInt("sizeChunk", 0);
	    for (int i = 0; i < CreateIndex.indices.length; ++i) {
		createNewFile(i);
	    }
	}

	private void createNewFile(int t) throws IOException {
	    if (fouts[t] != null) {
		fouts[t].close();
	    }

	    NumberFormat format = NumberFormat.getNumberInstance();
	    format.setGroupingUsed(false);
	    format.setMinimumIntegerDigits(5);
	    Path file = getDefaultWorkFile(context,
		    "_" + format.format(chunks[t]));
	    file = new Path(file.getParent(), CreateIndex.indices[t]
		    + "/index/" + file.getName());
	    fouts[t] = new GZIPOutputStream(FileSystem.get(
		    context.getConfiguration()).create(file));
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
		InterruptedException {
	    for (GZIPOutputStream fout : fouts) {
		if (fout != null) {
		    fout.close();
		}
	    }

	}

	@Override
	public void write(BytesWritable key, NullWritable value)
		throws IOException, InterruptedException {
	    byte[] b = key.getBytes();

	    counts[b[0]]++;
	    if (counts[b[0]] >= limit) {
		chunks[b[0]]++;
		createNewFile(b[0]);
		counts[b[0]] = 1;
	    }

	    fouts[b[0]].write(b, 1, 24);
	}

    }

    @Override
    public RecordWriter<BytesWritable, NullWritable> getRecordWriter(
	    TaskAttemptContext context) throws IOException,
	    InterruptedException {
	return new IndexRecordWriter(context);
    }

}