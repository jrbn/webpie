package mappers.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import data.Triple;
import data.TripleSource;

public class CleanTriplesDuplicatesMapper extends
		Mapper<TripleSource, Triple, Triple, TripleSource> {

	protected void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		context.write(value, key);
	}

}