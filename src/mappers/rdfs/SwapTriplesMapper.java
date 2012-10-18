package mappers.rdfs;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import data.Triple;
import data.TripleSource;

public class SwapTriplesMapper extends Mapper<TripleSource, Triple, Triple, TripleSource> {

	public void map(TripleSource key, Triple value, Context context) throws IOException, InterruptedException {
		context.write(value, key);
	}
}
