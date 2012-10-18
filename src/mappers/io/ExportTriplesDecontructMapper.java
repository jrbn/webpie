package mappers.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesDictReader;

import utils.NumberUtils;
import utils.TriplesUtils;

public class ExportTriplesDecontructMapper extends
		Mapper<BytesWritable, BytesWritable, LongWritable, BytesWritable> {

	private static Logger log = LoggerFactory
			.getLogger(ExportTriplesDecontructMapper.class);
	private Map<Long, String> commonResources = null;

	long tripleId = 0;
	LongWritable oKey = new LongWritable();
	BytesWritable textValue = new BytesWritable();

	byte[] obValue = new byte[9];
	BytesWritable oValue = new BytesWritable(obValue);

	protected void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		byte[] bValue = value.getBytes();
		if (key.getBytes()[0] == 1) { // Dictionary entry
			context.getCounter("input", "dictionary entry").increment(1);
			long bKey = NumberUtils.decodeLong(key.getBytes(), 1);
			// Exclude the common resources
			if (!commonResources.containsKey(bKey)) {
				oKey.set(bKey);
				textValue.setSize(value.getLength() + 1);
				System.arraycopy(bValue, 0, textValue.getBytes(), 1,
						value.getLength());
				textValue.getBytes()[0] = 0;
				context.write(oKey, textValue);
			}
		} else { // Triple.
			context.getCounter("input", "triple").increment(1);
			long id = tripleId++;
			NumberUtils.encodeLong(obValue, 1, id);

			// Subject
			long subject = NumberUtils.decodeLong(bValue, 0);
			if (commonResources.containsKey(subject)) {
				prepareCommonOutputValues(id, (byte) 1, subject);
				context.write(oKey, textValue);
			} else {
				obValue[0] = 1;
				oKey.set(subject);
				context.write(oKey, oValue);
			}

			// Predicate
			long predicate = NumberUtils.decodeLong(bValue, 8);
			if (commonResources.containsKey(predicate)) {
				prepareCommonOutputValues(id, (byte) 2, predicate);
				context.write(oKey, textValue);
			} else {
				obValue[0] = 2;
				oKey.set(predicate);
				context.write(oKey, oValue);
			}

			// Object
			long object = NumberUtils.decodeLong(bValue, 16);
			if (commonResources.containsKey(object)) {
				prepareCommonOutputValues(id, (byte) 3, object);
				context.write(oKey, textValue);
			} else {
				obValue[0] = 3;
				oKey.set(object);
				context.write(oKey, oValue);
			}
		}
	}

	private void prepareCommonOutputValues(long tripleId, byte position,
			long resource) {
		String text = commonResources.get(resource);
		byte[] bText = text.getBytes();
		textValue.setSize(bText.length + 1);
		byte[] bToOutput = textValue.getBytes();
		bToOutput[0] = position;
		System.arraycopy(bText, 0, bToOutput, 1, bText.length);
		oKey.set((tripleId * -1) - 1);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		if (commonResources == null) {
			String joinCommonResources = context.getConfiguration().get(
					"joinCommonResources");
			commonResources = FilesDictReader.readInvertedCommonResources(
					context.getConfiguration(), new Path(joinCommonResources));

			// Load also the already known URIs
			Map<String, Long> preloadedURIs = TriplesUtils.getInstance()
					.getPreloadedURIs();
			if (preloadedURIs != null) {
				Iterator<Entry<String, Long>> itr = preloadedURIs.entrySet()
						.iterator();
				while (itr.hasNext()) {
					Entry<String, Long> entry = itr.next();
					commonResources.put(entry.getValue().longValue(),
							entry.getKey());
				}
			}
		}
		log.debug("Size of the cache: " + commonResources.size());

		String taskId = context
				.getConfiguration()
				.get("mapred.task.id")
				.substring(
						context.getConfiguration().get("mapred.task.id")
								.indexOf("_m_") + 3);
		taskId = taskId.replaceAll("_", "");
		tripleId = Long.valueOf(taskId).longValue() << 32;
	}
}
