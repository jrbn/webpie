package mappers.owl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import utils.TriplesUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree.ByteResourceNode;
import data.Triple;
import data.TripleSource;

public class OWLEquivalenceSCSPMapper
		extends
		Mapper<TripleSource, Triple, LongWritable, ProtobufWritable<ByteResourceNode>> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLEquivalenceSCSPMapper.class);
	private LongWritable oKey = new LongWritable();
	private ProtobufWritable<ByteResourceNode> oValueContainer = ProtobufWritable
			.newInstance(ByteResourceNode.class);
	protected ByteResourceNode.Builder oValue = ByteResourceNode.newBuilder();

	private static Set<Long> subclassSchemaTriples = null;
	private static Set<Long> subpropSchemaTriples = null;

	@Override
	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		if (value.getSubject() == value.getObject())
			return;

		oKey.set(value.getSubject());
		oValue.setResource(value.getObject());
		oValue.setHistory(key.getHistory());

		if (value.getPredicate() == TriplesUtils.RDFS_SUBCLASS/*
				&& subclassSchemaTriples.contains(value.getObject())*/) {
			oValue.setId(0);
			oValueContainer.set(oValue.build());
			context.write(oKey, oValueContainer);
		}

		if (value.getPredicate() == TriplesUtils.RDFS_SUBPROPERTY
				&& subpropSchemaTriples.contains(value.getObject())) {
			oValue.setId(1);
			oValueContainer.set(oValue.build());
			context.write(oKey, oValueContainer);
		}

		if (value.getPredicate() == TriplesUtils.OWL_EQUIVALENT_CLASS) {
			oValue.setId(2);
			oValueContainer.set(oValue.build());
			context.write(oKey, oValueContainer);

			oKey.set(value.getObject());
			oValue.setResource(value.getSubject());
			oValueContainer.set(oValue.build());
			context.write(oKey, oValueContainer);
		}

		if (value.getPredicate() == TriplesUtils.OWL_EQUIVALENT_PROPERTY) {
			oValue.setId(3);
			oValueContainer.set(oValue.build());
			context.write(oKey, oValueContainer);

			oKey.set(value.getObject());
			oValue.setResource(value.getSubject());
			oValueContainer.set(oValue.build());
			context.write(oKey, oValueContainer);
		}
	}

	@Override
	public void setup(Context context) throws IOException {

		if (subpropSchemaTriples == null) {
			subpropSchemaTriples = new HashSet<Long>();
			FilesTriplesReader.loadSetIntoMemory(subpropSchemaTriples, context,
					"FILTER_ONLY_SUBPROP_SCHEMA", -1);
		}

		if (subclassSchemaTriples == null) {
			subclassSchemaTriples = new HashSet<Long>();
			FilesTriplesReader.loadSetIntoMemory(subclassSchemaTriples,
					context, "FILTER_ONLY_SUBCLASS_SCHEMA", -1);
		}
	}
}
