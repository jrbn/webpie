package mappers.rdfs;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import utils.NumberUtils;
import utils.TriplesUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree;
import data.Tree.ResourceNode;
import data.Triple;
import data.TripleSource;

public class RDFSSubPropInheritMapper
		extends
		Mapper<TripleSource, Triple, BytesWritable, ProtobufWritable<Tree.ResourceNode>> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubPropInheritMapper.class);
	protected static Map<Long, Integer> subpropSchemaTriples = null;

	protected BytesWritable oKey = new BytesWritable();
	protected ProtobufWritable<Tree.ResourceNode> oValueContainer = ProtobufWritable
			.newInstance(ResourceNode.class);
	protected ResourceNode.Builder oValue = ResourceNode.newBuilder();

	private int previousExecutionStep = -1;

	@Override
	protected void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		// Check if the triple is a subprop inheritance
		if (subpropSchemaTriples.containsKey(value.getPredicate())) {

			int schemaStep = subpropSchemaTriples.get(value.getPredicate());
			if (Math.max(schemaStep, key.getStep()) < previousExecutionStep)
				return;

			if (!value.isObjectLiteral())
				oKey.getBytes()[0] = 2;
			else
				oKey.getBytes()[0] = 3;
			NumberUtils.encodeLong(oKey.getBytes(), 1, value.getSubject());
			NumberUtils.encodeLong(oKey.getBytes(), 9, value.getObject());

			oValue.setResource(value.getPredicate());
			oValue.setHistory(key.getHistory());
			oValueContainer.set(oValue.build());

			context.write(oKey, oValueContainer);
		}

		// Check suprop transitivity
		if (value.getPredicate() == TriplesUtils.RDFS_SUBPROPERTY
				&& subpropSchemaTriples.containsKey(value.getObject())) {

			int schemaStep = subpropSchemaTriples.get(value.getObject());
			if (Math.max(schemaStep, key.getStep()) < previousExecutionStep)
				return;

			// Write the 05 + subject
			oKey.getBytes()[0] = 5;
			NumberUtils.encodeLong(oKey.getBytes(), 1, value.getSubject());

			oValue.setResource(value.getObject());
			oValue.setHistory(key.getHistory());
			oValueContainer.set(oValue.build());

			context.write(oKey, oValueContainer);
		}
	}

	@Override
	protected void setup(Context context) throws IOException {
		previousExecutionStep = context.getConfiguration().getInt(
				"reasoner.previousStep", -1);

		oKey.setSize(17);

		subpropSchemaTriples = FilesTriplesReader.loadTriplesWithStep(
				"FILTER_ONLY_SUBPROP_SCHEMA", context);
	}
}
