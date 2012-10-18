package mappers.owl;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;
import data.Triple;
import data.TripleSource;

public class OWLTransitivityMapper extends
		Mapper<TripleSource, Triple, BytesWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWLTransitivityMapper.class);
	byte[] keys = new byte[16];
	private BytesWritable oKey = new BytesWritable();
	byte[] values = new byte[17];
	private BytesWritable oValue = new BytesWritable();

	int level = 0;
	int baseLevel = 0;
	int minLevel = 0;
	int maxLevel = 0;

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		if (value.getSubject() != value.getObject() && !value.isObjectLiteral()) {

			if (key.getStep() == minLevel || key.getStep() == maxLevel) {
				NumberUtils.encodeLong(keys, 0, value.getPredicate());
				NumberUtils.encodeLong(keys, 8, value.getObject());
				oKey.set(keys, 0, 16);

				if (!key.isTransitiveActive())
					values[0] = 1;
				else
					values[0] = 0;

				NumberUtils.encodeLong(values, 1, key.getStep());
				NumberUtils.encodeLong(values, 9, value.getSubject());
				oValue.set(values, 0, 17);

				context.write(oKey, oValue);
			}

			if (key.getStep() > minLevel) {
				NumberUtils.encodeLong(keys, 0, value.getPredicate());
				NumberUtils.encodeLong(keys, 8, value.getSubject());
				oKey.set(keys, 0, 16);

				if (!key.isTransitiveActive())
					values[0] = 3;
				else
					values[0] = 2;
				NumberUtils.encodeLong(values, 1, key.getStep());
				NumberUtils.encodeLong(values, 9, value.getObject());
				oValue.set(values, 0, 17);

				context.write(oKey, oValue);
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