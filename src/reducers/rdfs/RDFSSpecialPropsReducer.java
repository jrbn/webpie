package reducers.rdfs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class RDFSSpecialPropsReducer extends
		Reducer<BytesWritable, LongWritable, TripleSource, Triple> {

	private TripleSource source = new TripleSource();
	private Triple oTriple = new Triple();

	@Override
	public void reduce(BytesWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		byte[] bKey = key.getBytes();
		Iterator<LongWritable> itr = values.iterator();
		while (itr.hasNext()) {// To avoid duplicates
			long value = itr.next().get();
			if (value == TriplesUtils.RDFS_LITERAL
					&& (bKey[0] == 0 || bKey[0] == 2))
				return;
			else if (value == TriplesUtils.RDFS_MEMBER
					&& (bKey[0] == 1 || bKey[0] == 4 || bKey[0] == 5))
				return;
			else if (value == TriplesUtils.RDFS_RESOURCE && bKey[0] == 3)
				return;
		}

		switch (bKey[0]) {
		case 1:
			oTriple.setSubject(NumberUtils.decodeLong(bKey, 1));
			oTriple.setPredicate(TriplesUtils.RDFS_SUBPROPERTY);
			oTriple.setObject(TriplesUtils.RDFS_MEMBER);
			oTriple.setObjectLiteral(false);
			context.write(source, oTriple);
			context.getCounter("RDFS derived triples", "subproperty of member")
					.increment(1);
			break;
		case 2:
			oTriple.setSubject(NumberUtils.decodeLong(bKey, 1));
			oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
			oTriple.setObject(TriplesUtils.RDFS_LITERAL);
			oTriple.setObjectLiteral(false);
			context.write(source, oTriple);
			context.getCounter("RDFS derived triples", "subclass of literal")
					.increment(1);
			break;
		case 3:
			oTriple.setSubject(NumberUtils.decodeLong(bKey, 1));
			oTriple.setPredicate(TriplesUtils.RDFS_SUBCLASS);
			oTriple.setObject(TriplesUtils.RDFS_RESOURCE);
			oTriple.setObjectLiteral(false);
			context.getCounter("RDFS derived triples", "subclass of resource")
					.increment(1);
			context.write(source, oTriple);
			break;
		case 4:
		case 5:
			oTriple.setSubject(NumberUtils.decodeLong(bKey, 1));
			oTriple.setPredicate(TriplesUtils.RDFS_MEMBER);
			oTriple.setObject(NumberUtils.decodeLong(bKey, 9));
			if (bKey[0] == 4)
				oTriple.setObjectLiteral(false);
			else
				oTriple.setObjectLiteral(true);
			context.getCounter("RDFS derived triples",
					"subproperty inheritance of member").increment(1);
			context.write(source, oTriple);
		default:
			break;
		}
	}

	@Override
	public void setup(Context context) {
		source.setDerivation(TripleSource.RDFS_SUBCLASS_SPECIAL);
		source.setAlreadyFiltered(true);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));
	}
}
