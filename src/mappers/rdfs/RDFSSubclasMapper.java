package mappers.rdfs;

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

public class RDFSSubclasMapper extends
		Mapper<TripleSource, Triple, BytesWritable, LongWritable> {

	protected static Logger log = LoggerFactory
			.getLogger(RDFSSubclasMapper.class);

	byte[] bKey = new byte[9];
	protected BytesWritable oKey = new BytesWritable();
	protected LongWritable oValue = new LongWritable(0);

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		oValue.set(value.getObject());
		if (value.getPredicate() == TriplesUtils.RDF_TYPE) {
			bKey[0] = 0;
		} else { // It's a subclass file
			bKey[0] = 1;
		}
		NumberUtils.encodeLong(bKey, 1, value.getSubject());
		oKey.set(bKey, 0, 9);
		context.write(oKey, oValue);
	}
}
