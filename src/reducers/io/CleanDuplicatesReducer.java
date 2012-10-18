package reducers.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;

import data.Triple;
import data.TripleSource;

public class CleanDuplicatesReducer extends
		Reducer<Triple, TripleSource, TripleSource, Triple> {

	int filterStep = -1;
	boolean setStep = false;
	boolean setDerivation = false;
//	Triple t = new Triple();

	@Override
	public void reduce(Triple key, Iterable<TripleSource> values,
			Context context) throws InterruptedException, IOException {
		TripleSource source = null;
		boolean isOriginal = false;

		Iterator<TripleSource> itr = values.iterator();
		while (itr.hasNext() && !isOriginal) {
			source = itr.next();
			if (source.getStep() <= filterStep || source.isAlreadyFiltered())
				isOriginal = true;
		}

		if (!isOriginal && source != null) {
			if (setStep)
				source.setStep(filterStep + 1);
			if (setDerivation)
				source.setAlreadyFiltered(true);
			
//			t.setSubject(key.subject);
//			t.setPredicate(key.predicate);
//			t.setObject(key.object);
//			t.setObjectLiteral(key.isObjectLiteral);
			context.write(source, key);
		}
	}

	@Override
	public void setup(Context context) {
		filterStep = context.getConfiguration().getInt("reasoner.filterStep",
				-1);
		setStep = context.getConfiguration().getBoolean(
				"reasoner.filterSetStep", false);
		setDerivation = context.getConfiguration().getBoolean(
				"reasoner.setDerivationKey", false);
	}

}
