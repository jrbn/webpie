package readers;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.FileUtils;

abstract public class MultiFilesReader<K, V> extends FileInputFormat<K, V> {

	protected static Logger log = LoggerFactory
			.getLogger(MultiFilesReader.class);

	protected static void listFiles(Path path, PathFilter filter,
			List<FileStatus> result, Configuration job) throws IOException {
		FileSystem fs = path.getFileSystem(job);
		FileStatus file = fs.getFileStatus(path);

		if (!file.isDir())
			throw new IOException("Path is not a dir");

		FileStatus[] children = fs.listStatus(path, filter);
		for (FileStatus child : children) {
			if (!child.isDir()) {
				result.add(child);
			} else {
				listFiles(child.getPath(), filter, result, job);
			}
		}
	}

	protected static ArrayList<FileStatus> recursiveListStatus(JobContext job)
			throws IOException {
		return recursiveListStatus(job, null);
	}

	public static ArrayList<FileStatus> recursiveListStatus(JobContext job,
			String filter) throws IOException {
		ArrayList<FileStatus> listFiles = new ArrayList<FileStatus>();

		Path[] dirs = getInputPaths(job);

		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}

		if (filter == null) {
			filter = job.getConfiguration().get("input.filter",
					"FILTER_ONLY_HIDDEN");
		}

		for (Path dir : dirs) {
			listFiles.addAll(recursiveListStatus(job.getConfiguration(), dir,
					filter));
		}

		return listFiles;
	}

	public static ArrayList<FileStatus> recursiveListStatus(Configuration conf,
			Path dir, String filter) throws IOException {
		ArrayList<FileStatus> listFiles = new ArrayList<FileStatus>();

		if (filter == null) {
			filter = "FILTER_ONLY_HIDDEN";
		}

		PathFilter jobFilter = null;
		try {
			Field field = FileUtils.class.getField(filter);
			jobFilter = (PathFilter) field.get(FileUtils.class);
		} catch (Exception e) {
			log.error("The specified filter was not loaded", e);
		}

		listFiles(dir, jobFilter, listFiles, conf);
		return listFiles;
	}

	private static final double SPLIT_SLOP = 1.1;

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		// Very old version

		// List<InputSplit> splits = new ArrayList<InputSplit>();
		// long totalLength = 0;
		// List<FileStatus> files = recursiveListStatus(job);
		// for (FileStatus file : files) {
		// totalLength += file.getLen();
		// }
		//
		//
		//
		// boolean isSplitable = job.getConfiguration().getBoolean(
		// "MultiFilesReader.isSplitable", true);
		// FileSystem fs = FileSystem.get(job.getConfiguration());
		// long splitSize = computeSplitSize(fs.getDefaultBlockSize(), Math.max(
		// getFormatMinSplitSize(), getMinSplitSize(job)),
		// getMaxSplitSize(job));
		// // long splitSize = (long)Math.ceil((double)totalLength / sizeSplit);
		//
		// MultiFilesSplit split = new MultiFilesSplit();
		// for (FileStatus file : files) {
		//
		// /*
		// * long splitSize = computeSplitSize(file.getBlockSize(), Math.max(
		// * getFormatMinSplitSize(), getMinSplitSize(job)),
		// * getMaxSplitSize(job));
		// */
		// BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
		// file.getLen());
		//
		// long startFile = 0;
		// while (startFile < file.getLen()) {
		//
		// long end = 0;
		// if (isSplitable) {
		// end = Math.min(startFile + splitSize - split.getLength(),
		// file.getLen());
		// } else {
		// end = file.getLen();
		// }
		//
		// split.addFile(file, startFile, end, blkLocations[getBlockIndex(
		// blkLocations, startFile)].getHosts());
		// startFile = end;
		// if (split.getLength() >= splitSize) {
		// splits.add(split);
		// split = new MultiFilesSplit();
		// }
		// }
		// }
		//
		// if (split.getLength() > 0) {
		// splits.add(split);
		// }
		//
		// return splits;

		List<InputSplit> splits = new ArrayList<InputSplit>();
		ArrayList<FileStatus> files = recursiveListStatus(job);

		long time = System.currentTimeMillis();

		// Sort files by size in order to reduce the number of tasks with
		// more files.
		FileStatus[] f = files.toArray(new FileStatus[files.size()]);
		Arrays.sort(f, new Comparator<FileStatus>() {
			@Override
			public int compare(FileStatus o1, FileStatus o2) {
				return (int) (o1.getLen() - o2.getLen());
			}
		});
		log.debug("Time sorting: " + (System.currentTimeMillis() - time));

		boolean isSplitable = job.getConfiguration().getBoolean(
				"MultiFilesReader.isSplitable", true);
		FileSystem fs = FileSystem.get(job.getConfiguration());

		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);

		long splitSize = computeSplitSize(fs.getDefaultBlockSize(), minSize,
				maxSize);

		MultiFilesSplit split = new MultiFilesSplit();
		// long threshold = (long) (splitSize * SPLIT_SLOP);
		for (FileStatus file : f) {

			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					file.getLen());

			// String blockLocations = "";
			// for (BlockLocation block : blkLocations) {
			// blockLocations += "(" + block.getOffset() + ","
			// + block.getLength() + ")";
			// }
			// log.debug("Block locations for file " + file.getPath().getName()
			// + " " + blockLocations);

			// File is very large. Starts with a new split.
			if (split.getLength() > 0
					&& (file.getLen() > (splitSize - split.getLength()))) {
				splits.add(split);
				split = new MultiFilesSplit();
			}

			long startFile = 0;
			while (startFile < file.getLen()) {
				long remaining = file.getLen() - startFile;
				long end = 0;
				if (isSplitable
						&& ((double) remaining)
								/ (splitSize - split.getLength()) > SPLIT_SLOP) {
					end = startFile + splitSize - split.getLength();
				} else {
					end = file.getLen();
				}

				split.addFile(file, startFile, end,
						blkLocations[getBlockIndex(blkLocations, startFile)]
								.getHosts());
				startFile = end;
				if (split.getLength() >= splitSize) {
					splits.add(split);
					split = new MultiFilesSplit();
				}
			}
		}

		if (split.getLength() > 0) {
			splits.add(split);
		}

		if (splits.size() == 0) {
			splits.add(new MultiFilesSplit());
		}

		// if (log.isDebugEnabled()) {
		// for (int i = 0; i < splits.size(); ++i) {
		// MultiFilesSplit s = (MultiFilesSplit) splits.get(i);
		// String detailFiles = "";
		// for (int j = 0; j < s.getFiles().size(); ++j) {
		// detailFiles += " ("
		// + s.getFiles().get(j).getPath().getName() + ","
		// + s.getStart(j) + "," + s.getEnds(j) + ")";
		// }
		// log.debug("Split " + i + " contains " + s.getFiles().size()
		// + " files. These are " + detailFiles);
		// }
		// }

		log.debug("Time creating splits: "
				+ (System.currentTimeMillis() - time));

		return splits;
	}

	public static void setSplitable(Configuration conf, boolean value) {
		conf.setBoolean("MultiFilesReader.isSplitable", value);
	}
}
