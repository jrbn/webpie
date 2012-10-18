package mappers.owl;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import data.Triple;
import data.TripleSource;

public class OWLSameAsMapper extends
		Mapper<TripleSource, Triple, LongWritable, LongWritable> {

	private LongWritable oKey = new LongWritable();
	private LongWritable oValue = new LongWritable();

	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		/* Source triple: s owl:sameAs o */
		if (value.getSubject() == value.getObject())
			return;

		oKey.set(value.getSubject());
		oValue.set(value.getObject());
		context.write(oKey, oValue);
		context.write(oValue, oKey);
	}
}
