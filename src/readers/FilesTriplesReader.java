package readers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;
import data.Tree.Node.Rule;
import data.Tree.ResourceNode;
import data.Triple;
import data.TripleSource;

public class FilesTriplesReader extends MultiFilesReader<TripleSource, Triple> {

	protected static Logger log = LoggerFactory
			.getLogger(FilesTriplesReader.class);

	public class FilesTriplesRecordReader extends
			RecordReader<TripleSource, Triple> {
		MultiFilesSplit split = null;
		TaskAttemptContext context = null;

		private SequenceFileRecordReader<TripleSource, Triple> rr = new SequenceFileRecordReader<TripleSource, Triple>();
		int i = 0;
		long readSoFar = 0;

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
		public Triple getCurrentValue() throws IOException,
				InterruptedException {
			return rr.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			long bytesCurrentFile = Math.round((double) (split.getEnds(Math
					.max(0, i - 1)) - split.getStart(Math.max(0, i - 1)))
					* rr.getProgress());
			return (float) ((double) (readSoFar + bytesCurrentFile) / split
					.getLength());
		}

		private void openNextFile() throws IOException, InterruptedException {
			// Close current record reader
			if (i > 0) {
				rr.close();
				readSoFar += split.getEnds(i - 1);
			}

			if (i < split.getFiles().size()) {
				FileStatus currentFile = split.getFiles().get(i);
				FileSplit fSplit = new FileSplit(currentFile.getPath(),
						split.getStart(i),
						split.getEnds(i) - split.getStart(i), null);
				rr.initialize(fSplit, context);
				++i;
			}
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.split = (MultiFilesSplit) split;
			this.context = context;
			readSoFar = 0;
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
	public RecordReader<TripleSource, Triple> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new FilesTriplesRecordReader();
	}

	public static boolean loadSetIntoMemory(Set<Long> schemaTriples,
			JobContext context, String filter, int previousStep)
			throws IOException {
		return loadSetIntoMemory(schemaTriples, context, filter, previousStep,
				false);
	}

	public static boolean loadSetIntoMemory(Set<Long> schemaTriples,
			JobContext context, String filter, int previousStep,
			boolean inverted) throws IOException {
		long startTime = System.currentTimeMillis();
		boolean schemaChanged = false;

		TripleSource key = new TripleSource();
		Triple value = new Triple();

		List<FileStatus> files = recursiveListStatus(context, filter);
		for (FileStatus file : files) {
			SequenceFile.Reader input = null;
			FileSystem fs = null;
			try {
				fs = file.getPath().getFileSystem(context.getConfiguration());
				input = new SequenceFile.Reader(fs, file.getPath(),
						context.getConfiguration());
				boolean nextTriple = false;
				do {
					nextTriple = input.next(key, value);
					if (nextTriple) {
						if (!inverted)
							schemaTriples.add(value.getSubject());
						else
							schemaTriples.add(value.getObject());
						if (key.getStep() > previousStep) {
							schemaChanged = true;
						}
					}
				} while (nextTriple);

			} finally {
				if (input != null) {
					input.close();
				}
			}
		}

		log.debug("Time for loading the schema files is "
				+ (System.currentTimeMillis() - startTime));
		return schemaChanged;
	}

	public static Map<Long, Collection<Long>> loadMapIntoMemory(String filter,
			JobContext context) throws IOException {
		return loadMapIntoMemory(filter, context, false);
	}

	public static Map<Long, Integer> loadTriplesWithStep(String filter,
			JobContext context) throws IOException {
		return loadTriplesWithStep(filter, context, false);
	}

	public static Map<Long, Collection<ResourceNode>> loadCompleteMapIntoMemoryWithInfo(
			String filter, JobContext context, boolean inverted)
			throws IOException {
		return loadCompleteMapIntoMemoryWithInfo(null, filter, context,
				inverted);
	}

	public static Map<Long, Collection<ResourceNode>> loadCompleteMapIntoMemoryWithInfo(
			Rule ruleToExclude, String filter, JobContext context,
			boolean inverted) throws IOException {

		Map<Long, Collection<ResourceNode>> schemaTriples = new HashMap<Long, Collection<ResourceNode>>();

		TripleSource key = new TripleSource();
		Triple value = new Triple();
		List<FileStatus> files = recursiveListStatus(context, filter);

		ResourceNode.Builder rn = ResourceNode.newBuilder();
		for (FileStatus file : files) {
			SequenceFile.Reader input = null;
			FileSystem fs = null;
			try {
				fs = file.getPath().getFileSystem(context.getConfiguration());
				input = new SequenceFile.Reader(fs, file.getPath(),
						context.getConfiguration());
				boolean nextTriple = false;
				do {
					nextTriple = input.next(key, value);
					if (nextTriple
							&& (ruleToExclude == null || key.getRule() != ruleToExclude)) {
						long tripleKey = 0;
						long tripleValue = 0;

						if (!inverted) {
							tripleKey = value.getSubject();
							tripleValue = value.getObject();
						} else {
							tripleKey = value.getObject();
							tripleValue = value.getSubject();
						}
						Collection<ResourceNode> cTriples = schemaTriples
								.get(tripleKey);

						if (cTriples == null) {
							cTriples = new ArrayList<ResourceNode>();
							schemaTriples.put(tripleKey, cTriples);
						}

						rn.setResource(tripleValue);
						rn.setHistory(key.getHistory());
						rn.setStep(key.getStep());
						cTriples.add(rn.build());
					}
				} while (nextTriple);
			} finally {
				if (input != null) {
					input.close();
				}
			}
		}

		return schemaTriples;
	}

	public static Map<Long, Integer> loadTriplesWithStep(String filter,
			JobContext context, boolean inverted) throws IOException {
		Map<Long, Integer> schemaTriples = new HashMap<Long, Integer>();
		TripleSource key = new TripleSource();
		Triple value = new Triple();

		List<FileStatus> files = recursiveListStatus(context, filter);
		for (FileStatus file : files) {
			SequenceFile.Reader input = null;
			FileSystem fs = null;
			try {
				fs = file.getPath().getFileSystem(context.getConfiguration());
				input = new SequenceFile.Reader(fs, file.getPath(),
						context.getConfiguration());
				boolean nextTriple = false;
				do {
					nextTriple = input.next(key, value);

					if (nextTriple) {
						long tripleKey = 0;
						if (!inverted)
							tripleKey = value.getSubject();
						else
							tripleKey = value.getObject();

						if (!schemaTriples.containsKey(tripleKey)) {
							schemaTriples.put(tripleKey, -1);
						}
						int version = schemaTriples.get(tripleKey);
						if (key.getStep() > version) {
							schemaTriples.put(tripleKey, key.getStep());
						}
					}
				} while (nextTriple);
			} finally {
				if (input != null) {
					input.close();
				}
			}
		}

		return schemaTriples;
	}

	public static Map<Long, Collection<byte[]>> loadMapIntoMemoryWithStep(
			String filter, JobContext context) throws IOException {
		Map<Long, Collection<byte[]>> schemaTriples = new HashMap<Long, Collection<byte[]>>();
		TripleSource key = new TripleSource();
		Triple value = new Triple();

		List<FileStatus> files = recursiveListStatus(context, filter);
		for (FileStatus file : files) {
			SequenceFile.Reader input = null;
			FileSystem fs = null;
			try {
				fs = file.getPath().getFileSystem(context.getConfiguration());
				input = new SequenceFile.Reader(fs, file.getPath(),
						context.getConfiguration());
				boolean nextTriple = false;
				do {
					nextTriple = input.next(key, value);

					if (nextTriple) {
						long tripleKey = 0;
						long tripleValue = 0;

						tripleKey = value.getSubject();
						tripleValue = value.getObject();

						Collection<byte[]> cTriples = schemaTriples
								.get(tripleKey);
						if (cTriples == null) {
							cTriples = new ArrayList<byte[]>();
							schemaTriples.put(tripleKey, cTriples);
						}
						byte[] bValue = new byte[12];
						NumberUtils.encodeLong(bValue, 0, tripleValue);
						NumberUtils.encodeInt(bValue, 8, key.getStep());
						cTriples.add(bValue);
					}
				} while (nextTriple);
			} finally {
				if (input != null) {
					input.close();
				}
			}
		}

		return schemaTriples;
	}

	public static Map<Long, Collection<Long>> loadMapIntoMemory(String filter,
			JobContext context, boolean inverted) throws IOException {
		Map<Long, Collection<Long>> schemaTriples = new HashMap<Long, Collection<Long>>();
		TripleSource key = new TripleSource();
		Triple value = new Triple();

		List<FileStatus> files = recursiveListStatus(context, filter);
		for (FileStatus file : files) {
			SequenceFile.Reader input = null;
			FileSystem fs = null;
			try {
				fs = file.getPath().getFileSystem(context.getConfiguration());
				input = new SequenceFile.Reader(fs, file.getPath(),
						context.getConfiguration());
				boolean nextTriple = false;
				do {
					nextTriple = input.next(key, value);

					if (nextTriple) {
						long tripleKey = 0;
						long tripleValue = 0;

						if (!inverted) {
							tripleKey = value.getSubject();
							tripleValue = value.getObject();
						} else {
							tripleKey = value.getObject();
							tripleValue = value.getSubject();
						}

						Collection<Long> cTriples = schemaTriples
								.get(tripleKey);
						if (cTriples == null) {
							cTriples = new ArrayList<Long>();
							schemaTriples.put(tripleKey, cTriples);
						}
						cTriples.add(tripleValue);
					}
				} while (nextTriple);
			} finally {
				if (input != null) {
					input.close();
				}
			}
		}

		return schemaTriples;
	}
}