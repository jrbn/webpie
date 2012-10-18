package mappers.owl2;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class OWL2JoinFirstRestMapper extends
		Mapper<TripleSource, Triple, LongWritable, BytesWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(OWL2JoinFirstRestMapper.class);
	private int currentExecution = -1;
	protected LongWritable oKey = new LongWritable();
	protected BytesWritable oValue = new BytesWritable();

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		if (key.getStep() > currentExecution) {
			oKey.set(value.getSubject());
			if (value.getPredicate() == TriplesUtils.RDF_FIRST) {
				oValue.getBytes()[0] = 0;
			} else { // RDF:REST
				oValue.getBytes()[0] = 1;
			}
			NumberUtils.encodeLong(oValue.getBytes(), 1, value.getObject());
			context.write(oKey, oValue);
		}
	}

	protected void setup(Context context) throws IOException {
		currentExecution = context.getConfiguration().getInt(
				"reasoner.currentExecution", -1);
		oValue.setSize(9);
	}
}
