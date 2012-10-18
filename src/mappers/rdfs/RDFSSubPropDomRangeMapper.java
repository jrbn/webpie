package mappers.rdfs;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;

import data.Triple;
import data.TripleSource;

public class RDFSSubPropDomRangeMapper extends
		Mapper<TripleSource, Triple, LongWritable, LongWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubPropDomRangeMapper.class);
	protected static Map<Long, Integer> domainSchemaTriples = null;
	protected static Map<Long, Integer> rangeSchemaTriples = null;

	protected LongWritable oValue = new LongWritable(0);
	protected LongWritable oKey = new LongWritable(0);

	private int previousExecutionStep = -1;

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		// Check if the predicate has a domain
		if (domainSchemaTriples.containsKey(value.getPredicate())) {

			int schemaStep = domainSchemaTriples.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousExecutionStep)
				return;

			oKey.set(value.getSubject());
			oValue.set(value.getPredicate() << 1);
			context.write(oKey, oValue);
		}

		// Check if the predicate has a range
		if (rangeSchemaTriples.containsKey(value.getPredicate())
				&& !value.isObjectLiteral()) {

			int schemaStep = rangeSchemaTriples.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousExecutionStep)
				return;

			oKey.set(value.getObject());
			oValue.set((value.getPredicate() << 1) | 1);
			context.write(oKey, oValue);
		}

	}

	@Override
	protected void setup(Context context) throws IOException {
		previousExecutionStep = context.getConfiguration().getInt(
				"reasoner.previousStep", -1);

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
