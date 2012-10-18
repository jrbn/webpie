package mappers.rdfs;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import data.Triple;
import data.TripleSource;

public class SwapTriplesMapper extends
		Mapper<TripleSource, Triple, Triple, TripleSource> {
	
//	CompressedTriple t = new CompressedTriple();
	
	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
//		t.subject = value.getSubject();
//		t.predicate = value.getPredicate();
//		t.object = value.getObject();
//		t.isObjectLiteral = value.isObjectLiteral();
//		context.write(t, key);
		context.write(value,key);
	}
}
