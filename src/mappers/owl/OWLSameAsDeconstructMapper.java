package mappers.owl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.FileUtils;
import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWLSameAsDeconstructMapper extends Mapper<TripleSource, Triple, LongWritable, BytesWritable> {

	private static Logger log = LoggerFactory.getLogger(OWLSameAsDeconstructMapper.class);

	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	private byte[] bValue = new byte[15];
	private long tripleId = 0;

	private static Map<Long,Long> commonResources = null;

	@Override
	public void map(TripleSource key, Triple value, Context context) throws IOException, InterruptedException {
		if (value.getPredicate() == TriplesUtils.OWL_SAME_AS) {
			oKey.set(value.getObject());
			bValue[0] = 4;
			NumberUtils.encodeLong(bValue, 1, value.getSubject());
			context.write(oKey, oValue);
		} else {
			Long commonValue = null;
			NumberUtils.encodeLong(bValue, 1, tripleId);
			NumberUtils.encodeInt(bValue, 9, key.getStep());
			//FIXME: bValue[13] = key.getStep();

			//Output the subject
			oKey.set(value.getSubject());
			if ((commonValue = commonResources.get(value.getSubject())) != null) {
				bValue[14] = 1;
				if (commonValue != 0) oKey.set(commonValue);
			} else {
				bValue[14] = 0;
			}
			bValue[0] = 0;
			context.write(oKey, oValue);

			//Output the predicate
			oKey.set(value.getPredicate());
			if ((commonValue = commonResources.get(value.getPredicate())) != null) {
				bValue[14] = 1;
				if (commonValue != 0) oKey.set(commonValue);
			} else {
				bValue[14] = 0;
			}
			bValue[0] = 1;
			context.write(oKey, oValue);

			//Output the object
			oKey.set(value.getObject());
			if ((commonValue = commonResources.get(value.getObject())) != null) {
				bValue[14] = 1;
				if (commonValue != 0) oKey.set(commonValue);
			} else {
				bValue[14] = 0;
			}
			if (!value.isObjectLiteral())
				bValue[0] = 2;
			else
				bValue[0] = 3;
			context.write(oKey, oValue);

			++tripleId;
		}
	}

	@Override
	public void setup(Context context) {
		oValue = new BytesWritable(bValue);

		try {
			String taskId = context.getConfiguration().get("mapred.task.id").substring(context.getConfiguration().get("mapred.task.id").indexOf("_m_") + 3);
			taskId = taskId.replaceAll("_", "");
			tripleId = Long.valueOf(taskId).longValue() << 32;
			log.debug("Assign triple's counter to: " + tripleId);
		} catch (Exception e) {
			log.error("Impossible to parse the task id", e);
		}

		if (commonResources == null) {
			String sPool =context.getConfiguration().get("mapred.input.dir");
			try {
				long time = System.currentTimeMillis();
				commonResources = loadCommonURIs(sPool, context);
				log.info("Common resources loaded: " + commonResources.size() + " in " + (System.currentTimeMillis() - time));
			} catch (Exception e) {
				log.error("Error in loading the common URIs");
				commonResources = new HashMap<Long, Long>();
			}
		} else {
			log.info("Common resources already loaded!");
		}

	}

	protected Map<Long, Long> loadCommonURIs(String path, Context context) throws IOException {

		FileStatus[] files = FileSystem.get(context.getConfiguration()).listStatus(new Path(path, "_commonResources"),
					FileUtils.FILTER_ONLY_HIDDEN);

		Map<Long, Long> commonURIs = new HashMap<Long, Long>();
		for(FileStatus file : files) {
			SequenceFile.Reader input = null;
			FileSystem fs = null;
			try {
				fs = file.getPath().getFileSystem(context.getConfiguration());
				input = new SequenceFile.Reader(fs, file.getPath(), context.getConfiguration());

				boolean nextValue = false;
				LongWritable key = new LongWritable();
				LongWritable value = new LongWritable();
				nextValue = input.next(key, value);
				while (nextValue) {
					commonURIs.put(key.get(), value.get());
					nextValue = input.next(key, value);
				}

			} catch (IOException e) {
				log.error("Failed reading file", e);
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

		log.debug("Number of elements in the cache: " + commonURIs.size());
		return commonURIs;
	}
}
