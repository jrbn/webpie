package mappers.rdfs;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import utils.NumberUtils;
import data.Triple;
import data.TripleSource;

public class RDFSSubPropDomRangeMapper extends
		Mapper<TripleSource, Triple, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubPropDomRangeMapper.class);
	protected static Map<Long, Integer> domainSchemaTriples = null;
	protected static Map<Long, Integer> rangeSchemaTriples = null;

	protected LongWritable oKey = new LongWritable(0);
	protected BytesWritable oValue = new BytesWritable();

	private int previousExecutionStep = -1;

	@Override
	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		// Check if the predicate has a domain
		if (domainSchemaTriples.containsKey(value.getPredicate())) {

			int schemaStep = domainSchemaTriples.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousExecutionStep)
				return;

			oKey.set(value.getSubject());
			byte[] history = key.getHistory().toByteArray();
			oValue.setSize(history.length + 9);
			oValue.getBytes()[0] = 0;
			NumberUtils.encodeLong(oValue.getBytes(), 1, value.getPredicate());
			System.arraycopy(history, 0, oValue.getBytes(), 9, history.length);
			context.write(oKey, oValue);
		}

		// Check if the predicate has a range
		if (rangeSchemaTriples.containsKey(value.getPredicate())
				&& !value.isObjectLiteral()) {

			int schemaStep = rangeSchemaTriples.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousExecutionStep)
				return;

			oKey.set(value.getObject());

			byte[] history = key.getHistory().toByteArray();
			oValue.setSize(history.length + 9);
			oValue.getBytes()[0] = 1;
			NumberUtils.encodeLong(oValue.getBytes(), 1, value.getPredicate());
			System.arraycopy(history, 0, oValue.getBytes(), 9, history.length);
			context.write(oKey, oValue);
		}

	}

	@Override
	protected void setup(Context context) throws IOException {
		previousExecutionStep = context.getConfiguration().getInt(
				"reasoner.previousStep", -1);
		oValue.setSize(9);

		if (domainSchemaTriples == null) {
			domainSchemaTriples = FilesTriplesReader.loadTriplesWithStep(
					"FILTER_ONLY_DOMAIN_SCHEMA", context);
		}

		if (rangeSchemaTriples == null) {
			rangeSchemaTriples = FilesTriplesReader.loadTriplesWithStep(
					"FILTER_ONLY_RANGE_SCHEMA", context);
		}
	}
}
