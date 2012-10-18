package mappers.owl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import readers.MultiFilesReader;

import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWLAllSomeValuesMapper extends
		Mapper<TripleSource, Triple, BytesWritable, BytesWritable> {

	private static Logger log = LoggerFactory
			.getLogger(OWLAllSomeValuesMapper.class);

	private byte[] bKey = new byte[17];
	private BytesWritable oKey = new BytesWritable(bKey);

	private byte[] bValue = new byte[13];
	private BytesWritable oValue = new BytesWritable(bValue);

	private static Map<Long, Collection<byte[]>> allValues = null;
	private static Map<Long, Collection<byte[]>> someValues = null;
	private static Set<Long> onPropertyAll = null;
	private static Set<Long> onPropertySome = null;

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		if (value.getPredicate() == TriplesUtils.RDF_TYPE) {

			if (someValues.containsKey(value.getObject())) {
				Collection<byte[]> values = someValues.get(value.getObject());
				Iterator<byte[]> itr = values.iterator();
				bKey[0] = 2;
				bValue[0] = 1;
				NumberUtils.encodeLong(bKey, 9, value.getSubject());
				while (itr.hasNext()) {
					byte[] bytes = itr.next();
					System.arraycopy(bytes, 0, bKey, 1, 8);
					System.arraycopy(bytes, 8, bValue, 1, 8);
					NumberUtils.encodeInt(
							bValue,
							9,
							Math.max(NumberUtils.decodeInt(bytes, 16),
									key.getStep()));
					context.write(oKey, oValue);
				}
			}

			if (allValues.containsKey(value.getObject())) {
				Collection<byte[]> values = allValues.get(value.getObject());
				Iterator<byte[]> itr = values.iterator();
				bKey[0] = 1;
				bValue[0] = 1;
				NumberUtils.encodeLong(bKey, 9, value.getSubject());
				while (itr.hasNext()) {
					byte[] bytes = itr.next();
					System.arraycopy(bytes, 0, bKey, 1, 8);
					System.arraycopy(bytes, 8, bValue, 1, 8);
					NumberUtils.encodeInt(
							bValue,
							9,
							Math.max(NumberUtils.decodeInt(bytes, 16),
									key.getStep()));
					context.write(oKey, oValue);
				}
			}

		} else {

			if (onPropertySome.contains(value.getPredicate())) {
				// Rule 15 - someValuesFrom
				bKey[0] = 2;
				bValue[0] = 0;
				NumberUtils.encodeLong(bKey, 1, value.getPredicate());
				NumberUtils.encodeLong(bKey, 9, value.getObject());
				NumberUtils.encodeLong(bValue, 1, value.getSubject());
				NumberUtils.encodeInt(bValue, 9, key.getStep());
				context.write(oKey, oValue);
			}

			if (onPropertyAll.contains(value.getPredicate())) {
				// Rule 16 - allValuesFrom
				bKey[0] = 1;
				bValue[0] = 0;
				NumberUtils.encodeLong(bKey, 1, value.getPredicate());
				NumberUtils.encodeLong(bKey, 9, value.getSubject());
				NumberUtils.encodeLong(bValue, 1, value.getObject());
				NumberUtils.encodeInt(bValue, 9, key.getStep());
				context.write(oKey, oValue);
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		List<FileStatus> filesProperty = MultiFilesReader.recursiveListStatus(
				context, "FILTER_ONLY_OWL_ON_PROPERTY");

		Map<Long, Collection<byte[]>> allValuesTmp = FilesTriplesReader
				.loadMapIntoMemoryWithStep("FILTER_ONLY_OWL_ALL_VALUES",
						context);
		Map<Long, Collection<byte[]>> someValuesTmp = FilesTriplesReader
				.loadMapIntoMemoryWithStep("FILTER_ONLY_OWL_SOME_VALUES",
						context);

		onPropertyAll = new HashSet<Long>();
		onPropertySome = new HashSet<Long>();
		someValues = new HashMap<Long, Collection<byte[]>>();
		allValues = new HashMap<Long, Collection<byte[]>>();
		makeJoin(filesProperty.toArray(new FileStatus[filesProperty.size()]),
				context, someValuesTmp, allValuesTmp, someValues, allValues,
				onPropertySome, onPropertyAll);
	}

	protected void makeJoin(FileStatus[] files, Context context,
			Map<Long, Collection<byte[]>> someValuesTmp,
			Map<Long, Collection<byte[]>> allValuesTmp,
			Map<Long, Collection<byte[]>> someValues,
			Map<Long, Collection<byte[]>> allValues, Set<Long> onPropertySome,
			Set<Long> onPropertyAll) {
		TripleSource key = new TripleSource();
		Triple value = new Triple();

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
						// Check if there is a match with someValuesFrom and
						// allValuesFrom
						if (someValuesTmp.containsKey(value.getSubject())) {
							Collection<byte[]> col = someValuesTmp.get(value
									.getSubject());
							if (col != null) {
								Iterator<byte[]> itr = col.iterator();
								while (itr.hasNext()) {
									byte[] w_step = itr.next();
									long w = NumberUtils.decodeLong(w_step, 0);
									int step = NumberUtils.decodeInt(w_step, 8);

									byte[] bytes = new byte[20];
									// Save v and p there.
									NumberUtils.encodeLong(bytes, 0,
											value.getObject());
									NumberUtils.encodeLong(bytes, 8,
											value.getSubject());
									NumberUtils.encodeInt(bytes, 16,
											Math.max(step, key.getStep())); // Max
																			// step
									// between
									// onProperty
									// and
									// someValues
									// triples

									Collection<byte[]> cValues = someValues
											.get(w);
									if (cValues == null) {
										cValues = new ArrayList<byte[]>();
										someValues.put(w, cValues);
									}
									cValues.add(bytes);
									onPropertySome.add(value.getObject());
								}
							}
						}

						if (allValuesTmp.containsKey(value.getSubject())) {
							Collection<byte[]> col = allValuesTmp.get(value
									.getSubject());
							if (col != null) {
								Iterator<byte[]> itr = col.iterator();
								while (itr.hasNext()) {
									byte[] w_step = itr.next();
									long w = NumberUtils.decodeLong(w_step, 0);
									int step = NumberUtils.decodeInt(w_step, 8);

									byte[] bytes = new byte[20];
									// Save v and p there.
									NumberUtils.encodeLong(bytes, 0,
											value.getObject());
									NumberUtils.encodeLong(bytes, 8, w);
									NumberUtils.encodeInt(bytes, 16,
											Math.max(step, key.getStep())); // Max
																			// step
									// between
									// onProperty
									// and
									// allValues
									// triples

									Collection<byte[]> cValues = allValues
											.get(value.getSubject());
									if (cValues == null) {
										cValues = new ArrayList<byte[]>();
										allValues.put(value.getSubject(),
												cValues);
									}
									cValues.add(bytes);
									onPropertyAll.add(value.getObject());
								}
							}
						}
					}
				} while (nextTriple);

			} catch (IOException e) {
				log.error("Failed reading schema files", e);
			} finally {
				try {
					if (input != null) {
						input.close();
					}
				} catch (IOException e) {
					log.error("Failed in closing the input stream");
				}
			}
		}
	}
}
