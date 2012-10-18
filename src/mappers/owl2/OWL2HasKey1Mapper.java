package mappers.owl2;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;

import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWL2HasKey1Mapper extends
		Mapper<TripleSource, Triple, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2HasKey1Mapper.class);
	protected BytesWritable oKey = new BytesWritable();
	protected BytesWritable oValue = new BytesWritable();

	protected Map<Long, Collection<Long>> hasKeyClasses = null;
	protected Map<Long, Collection<byte[]>> lists = null;

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		if (value.getPredicate() == TriplesUtils.RDF_TYPE) {
			if (hasKeyClasses.containsKey(value.getObject())) {
				Collection<Long> col = hasKeyClasses.get(value.getObject());
				NumberUtils.encodeLong(oKey.getBytes(), 0, value.getSubject());
				oValue.getBytes()[0] = 1; // Ok flag
				for (long el : col) {
					NumberUtils.encodeLong(oKey.getBytes(), 8, el);
					context.write(oKey, oValue);
				}
			}
		} else {
			// Check whether the property matches the list
			if (lists.containsKey(value.getPredicate())) {
				Collection<byte[]> col = lists.get(value.getPredicate());
				NumberUtils.encodeLong(oKey.getBytes(), 0, value.getSubject());
				oValue.getBytes()[0] = 0;
				NumberUtils.encodeLong(oValue.getBytes(), 1, value.getObject());
				for (byte[] el : col) {
					System.arraycopy(el, 0, oKey.getBytes(), 8, 8);
					System.arraycopy(el, 8, oValue.getBytes(), 9, 8); // Position
																		// +
																		// length
					context.write(oKey, oValue);
				}
			}
		}
	}

	protected void setup(Context context) throws IOException {
		oKey.setSize(16);
		oValue.setSize(17);

		// Load schema in memory
		if (hasKeyClasses == null) { // Load the schema in memory
			lists = new HashMap<Long, Collection<byte[]>>();
			hasKeyClasses = FilesTriplesReader.loadMapIntoMemory(
					"FILTER_ONLY_OWL_HAS_KEY", context, false);
			Set<Long> allowedLists = new HashSet<Long>();
			for (Collection<Long> col : hasKeyClasses.values()) {
				for (long value : col) {
					allowedLists.add(value);
				}
			}

			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path[] paths = FileInputFormat.getInputPaths(context);
			FileStatus[] files = fs.listStatus(new Path(paths[0],
					"_lists/dir-current"), new OutputLogFilter());
			BytesWritable key = new BytesWritable();
			BytesWritable value = new BytesWritable();
			for (FileStatus file : files) {
				SequenceFile.Reader input = new SequenceFile.Reader(fs,
						file.getPath(), context.getConfiguration());
				boolean nextList = false;
				do {
					nextList = input.next(key, value);
					if (nextList) {
						long listHeader = NumberUtils.decodeLong(
								key.getBytes(), 0);
						if (allowedLists.contains(listHeader)) {
							for (int i = 0; i < value.getLength(); i += 8) {
								long prop = NumberUtils.decodeLong(
										value.getBytes(), i);
								if (!lists.containsKey(prop)) {
									lists.put(prop, new LinkedList<byte[]>());
								}
								Collection<byte[]> col = lists.get(prop);
								byte[] el = new byte[16];
								NumberUtils.encodeLong(el, 0, listHeader);
								NumberUtils.encodeInt(el, 8, i / 8);
								NumberUtils.encodeInt(el, 12,
										value.getLength() / 8);
								col.add(el);
							}
						}
					}
				} while (nextList);
			}

		}
	}
}
