package mappers.owl;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree.ByteResourceNode;
import data.Triple;
import data.TripleSource;

public class OWLTransitivityMapper
		extends
		Mapper<TripleSource, Triple, BytesWritable, ProtobufWritable<ByteResourceNode>> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLTransitivityMapper.class);
	byte[] keys = new byte[16];
	private BytesWritable oKey = new BytesWritable();

	private ProtobufWritable<ByteResourceNode> oValueContainer = ProtobufWritable
			.newInstance(ByteResourceNode.class);
	protected ByteResourceNode.Builder oValue = ByteResourceNode.newBuilder();

	int level = 0;
	int baseLevel = 0;
	int minLevel = 0;
	int maxLevel = 0;

	@Override
	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		if (value.getSubject() != value.getObject() && !value.isObjectLiteral()) {

			if (key.getStep() == minLevel || key.getStep() == maxLevel) {
				NumberUtils.encodeLong(keys, 0, value.getPredicate());
				NumberUtils.encodeLong(keys, 8, value.getObject());
				oKey.set(keys, 0, 16);

				if (!key.isTransitiveActive())
					oValue.setId(1);
				else
					oValue.setId(0);

				oValue.setResource(value.getSubject());
				oValue.setHistory(key.getHistory());
				oValueContainer.set(oValue.build());
				context.write(oKey, oValueContainer);
			}

			if (key.getStep() > minLevel) {
				NumberUtils.encodeLong(keys, 0, value.getPredicate());
				NumberUtils.encodeLong(keys, 8, value.getSubject());
				oKey.set(keys, 0, 16);

				if (!key.isTransitiveActive())
					oValue.setId(3);
				else
					oValue.setId(2);

				oValue.setHistory(key.getHistory());
				oValue.setResource(value.getObject());
				oValueContainer.set(oValue.build());
				context.write(oKey, oValueContainer);
			}
		}
	}

	@Override
	public void setup(Context context) {
		level = context.getConfiguration().getInt(
				"reasoning.transitivityLevel", 0);
		baseLevel = context.getConfiguration().getInt("reasoning.baseLevel", 0) - 1;
		minLevel = Math.max(1, (int) Math.pow(2, level - 2)) + baseLevel;
		maxLevel = Math.max(1, (int) Math.pow(2, level - 1)) + baseLevel;
		if (level == 1)
			minLevel = 0 + baseLevel;
	}
}