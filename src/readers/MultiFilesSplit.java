package readers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class MultiFilesSplit extends InputSplit implements Writable {

	private long totalLength = 0;
	private List<FileStatus> list = new ArrayList<FileStatus>();
	private List<Long> starts = new ArrayList<Long>();
	private List<Long> ends = new ArrayList<Long>();
	private Set<String> hosts = new HashSet<String>();

	@Override
	public long getLength() {
		return totalLength;
	}

	public long getStart(int i) {
		return starts.get(i);
	}

	public long getEnds(int i) {
		return ends.get(i);
	}

	public void addFile(FileStatus file, long start, long end, String[] hosts) {
		list.add(file);
		starts.add(start);
		ends.add(end);
		totalLength += end - start;
		for (String host : hosts) {
			this.hosts.add(host);
		}
	}

	public List<FileStatus> getFiles() {
		return list;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return hosts.toArray(new String[hosts.size()]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		totalLength = in.readLong();
		int numFiles = in.readInt();
		list.clear();
		starts.clear();
		ends.clear();
		for (int i = 0; i < numFiles; ++i) {
			FileStatus file = new FileStatus();
			file.readFields(in);
			list.add(file);
			starts.add(in.readLong());
			ends.add(in.readLong());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(totalLength);
		out.writeInt(list.size());

		Iterator<Long> itrStart = starts.iterator();
		Iterator<Long> itrEnds = ends.iterator();
		for (FileStatus file : list) {
			file.write(out);
			out.writeLong(itrStart.next());
			out.writeLong(itrEnds.next());
		}
	}

}
