package mappers.owl2;


import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import data.Triple;
import data.TripleSource;

public class OWL2PropChainExtractMapper extends
		Mapper<TripleSource, Triple, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2PropChainExtractMapper.class);

	protected BytesWritable oKey = new BytesWritable();
	protected BytesWritable oDuplKey = new BytesWritable();
	protected BytesWritable oValue = new BytesWritable();

	protected Map<Long, Collection<byte[]>> properties = null;
	protected Set<Long> chainProperties = null;

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		
			if (properties.containsKey(value.getPredicate())) {
				Collection<byte[]> matches = properties.get(value
						.getPredicate());
				NumberUtils.encodeLong(oValue.getBytes(), 0, value.getSubject());
				NumberUtils.encodeLong(oValue.getBytes(), 16, value.getObject());
				for(byte[] match : matches) {
					NumberUtils.encodeLong(oValue.getBytes(), 8, NumberUtils.decodeLong(match, 0));
					NumberUtils.encodeInt(oKey.getBytes(), 4, NumberUtils.decodeInt(match, 8));
					NumberUtils.encodeInt(oKey.getBytes(), 8, NumberUtils.decodeInt(match, 12));
					context.write(oKey,oValue);
				}
			}
			
			if (chainProperties.contains(value.getPredicate())) {
				//Extract it to check the duplicates
				NumberUtils.encodeLong(oValue.getBytes(), 0, value.getSubject());
				NumberUtils.encodeLong(oValue.getBytes(), 8, value.getPredicate());
				NumberUtils.encodeLong(oValue.getBytes(), 16, value.getObject());				
				context.write(oDuplKey, oValue);
			}
	}

	protected void setup(Context context) throws IOException {
		oKey.setSize(12);
		NumberUtils.encodeInt(oKey.getBytes(), 0, 1);
		oValue.setSize(24);

		if (properties == null) { // Load the schema in memory
			properties = new HashMap<Long, Collection<byte[]>>();

			 Map<Long, Collection<Long>> chainProps = FilesTriplesReader
					.loadMapIntoMemory("FILTER_ONLY_OWL_CHAIN_PROPERTIES",
							context, true);
			 chainProperties = new HashSet<Long>();
			 FilesTriplesReader.loadSetIntoMemory(chainProperties, context, "FILTER_ONLY_OWL_CHAIN_PROPERTIES", -1);

			// Read all the lists and check whether some match with the chain
			// properties
			FileSystem fs = FileSystem.get(context.getConfiguration());

			Path[] paths = FileInputFormat.getInputPaths(context);
			FileStatus[] files = fs.listStatus(new Path(paths[0], "_lists/dir-current"),
					new OutputLogFilter());
			BytesWritable key = new BytesWritable();
			BytesWritable value = new BytesWritable();
			for (FileStatus file : files) {
				/*
				 * Open the file and check whether it matches with the schema
				 * loaded before
				 */
				SequenceFile.Reader input = new SequenceFile.Reader(fs, file
						.getPath(), context.getConfiguration());
				boolean nextList = false;
				do {
					nextList = input.next(key, value);
					if (nextList) {
						long listHeader = NumberUtils.decodeLong(
								key.getBytes(), 0);
						if (chainProps.containsKey(listHeader)) {
							Collection<Long> axiomProps = chainProps
									.get(listHeader);
							for (int i = 0; i < value.getLength(); i += 8) {
								long prop = NumberUtils.decodeLong(value
										.getBytes(), i);
								if (!properties.containsKey(prop)) {
									properties.put(prop,
											new LinkedList<byte[]>());
								}
								Collection<byte[]> col = properties.get(prop);
								Iterator<Long> itr = axiomProps.iterator();
								while (itr.hasNext()) {
									long axiomProp = itr.next();
									byte[] valueToInsert = new byte[16];
									NumberUtils.encodeLong(valueToInsert, 0,
											axiomProp);
									NumberUtils.encodeInt(valueToInsert, 8,
											i / 8);
									NumberUtils.encodeInt(valueToInsert, 12,
											value.getLength() / 8);
									col.add(valueToInsert);
								}
							}
						}
					}
				} while (nextList);
			}

		}

	}
}
