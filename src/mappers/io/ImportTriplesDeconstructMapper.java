package mappers.io;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesDictReader;

import utils.NumberUtils;
import utils.TriplesUtils;

public class ImportTriplesDeconstructMapper extends
		Mapper<BytesWritable, BytesWritable, Text, LongWritable> {

	private static Logger log = LoggerFactory
			.getLogger(ImportTriplesDeconstructMapper.class);
	private long counter = 0;
	static private Map<String, Long> commonURIs = null;
	private Text oKey = new Text();
	private LongWritable oValue = new LongWritable();
	private Random random = new Random();
	boolean rewriteBlankNodes = true;

	protected void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		if (key.getBytes()[0] == 0) {
			try {
				String sKey = new String(key.getBytes(), 1, key.getLength() - 1);
				String sValue = new String(value.getBytes(), 0,
						value.getLength());
				String nodes[] = TriplesUtils.parseTriple(sValue, sKey,
						rewriteBlankNodes);
				// Assign unique id to triple
				long id = counter++;
				// Return single nodes
				for (int i = 0; i < 3; ++i) {
					String nodeValue = nodes[i];
					if (i == 2 && nodes[2].charAt(0) == '"')
						i = 3; // Marker to sign that is object and literal
					long nodeIdInTriple = 0;
					nodeIdInTriple = (id << 2) | (long) i; // Encode the
															// position in
					// the triple id - last
					// 2 bit

					// Check if it is a common URIs.
					if (commonURIs.containsKey(nodeValue)) {
						Long valueLong = commonURIs.get(nodeValue);
						nodeValue = "@FAKE" + random.nextInt(100) + "-"
								+ valueLong;
					}

					oKey.set(nodeValue);
					oValue.set(nodeIdInTriple);
					context.write(oKey, oValue);
				}
			} catch (Exception e) {
				context.setStatus("Failed parsing triple");
				log.error(e.getMessage());
			}
		} else { // Dictionary
			oKey.set(new String(value.getBytes(), 0, value.getLength()));
			long id = NumberUtils.decodeLong(key.getBytes(), 1);
			oValue.set(id * -1);
			context.write(oKey, oValue);
		}
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		// Init the counter
		String taskId = context
				.getConfiguration()
				.get("mapred.task.id")
				.substring(
						context.getConfiguration().get("mapred.task.id")
								.indexOf("_m_") + 3);
		taskId = taskId.replaceAll("_", "");
		counter = (Long.valueOf(taskId).longValue()) << 32;

		rewriteBlankNodes = context.getConfiguration().getBoolean(
				"ImportTriples.rewriteBlankNodes", true);

		// Init the cache
		if (commonURIs == null) {
			long time = System.currentTimeMillis();
			commonURIs = FilesDictReader.readCommonResources(context
					.getConfiguration(), new Path(context.getConfiguration()
					.get("commonResources")));
			log.debug("Time to load popular URIs = "
					+ (System.currentTimeMillis() - time));
			commonURIs.putAll(TriplesUtils.getInstance().getPreloadedURIs());
		} else {
			log.debug("Table already loaded");
		}
		log.debug("Size of the common resources: " + commonURIs.size());
	}
}