package readers;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
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

	protected static List<FileStatus> recursiveListStatus(JobContext job)
			throws IOException {
		return recursiveListStatus(job, null);
	}

	public static List<FileStatus> recursiveListStatus(JobContext job,
			String filter) throws IOException {
		List<FileStatus> listFiles = new ArrayList<FileStatus>();

		Path[] dirs = getInputPaths(job);
		
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}
		
		if (filter == null) {
			filter = job.getConfiguration().get("input.filter",
					"FILTER_ONLY_HIDDEN");
		}

		for (Path dir : dirs) {
			listFiles.addAll(recursiveListStatus(job.getConfiguration(), dir, filter));
		}
		
		return listFiles;
	}
	
	public static List<FileStatus> recursiveListStatus(Configuration conf,
			Path dir, String filter) throws IOException {
		List<FileStatus> listFiles = new ArrayList<FileStatus>();

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

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		List<InputSplit> splits = new ArrayList<InputSplit>();
		long totalLength = 0;
		List<FileStatus> files = recursiveListStatus(job);
		for (FileStatus file : files) {
			totalLength += file.getLen();
		}
		
		

		boolean isSplitable = job.getConfiguration().getBoolean(
				"MultiFilesReader.isSplitable", true);
		FileSystem fs = FileSystem.get(job.getConfiguration());
		long splitSize = computeSplitSize(fs.getDefaultBlockSize(), Math.max(
				getFormatMinSplitSize(), getMinSplitSize(job)),
				getMaxSplitSize(job));
		// long splitSize = (long)Math.ceil((double)totalLength / sizeSplit);

		MultiFilesSplit split = new MultiFilesSplit();
		for (FileStatus file : files) {

			/*
			 * long splitSize = computeSplitSize(file.getBlockSize(), Math.max(
			 * getFormatMinSplitSize(), getMinSplitSize(job)),
			 * getMaxSplitSize(job));
			 */
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					file.getLen());

			long startFile = 0;
			while (startFile < file.getLen()) {

				long end = 0;
				if (isSplitable) {
					end = Math.min(startFile + splitSize - split.getLength(),
							file.getLen());
				} else {
					end = file.getLen();
				}

				split.addFile(file, startFile, end, blkLocations[getBlockIndex(
						blkLocations, startFile)].getHosts());
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

		return splits;
	}

	public static void setSplitable(Configuration conf, boolean value) {
		conf.setBoolean("MultiFilesReader.isSplitable", value);
	}
}
