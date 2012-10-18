package jobs.blocks;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteDuplicatesBlock extends ExecutionBlock {

	@Override
	public void performJobs(int executionStep) throws IOException,
			InterruptedException, ClassNotFoundException {

		if (getStrategy() != STRATEGY_CLEAN_DUPL_ALWAYS) {
			setFilteredDerivation(deleteDuplicatedTriples(pool.toString(),
					pool.toString() + OWL_NOT_FILTERED_DIR,
					"FILTER_ONLY_HIDDEN", pool.toString() + OWL_OUTPUT_DIR
							+ "/dir-final-derivation", getFilterFromStep(),
					true, false, true));
		}
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(pool + ExecutionBlock.OWL_NOT_FILTERED_DIR), true);
		fs.delete(new Path(pool + ExecutionBlock.RDFS_NOT_FILTERED_DIR), true);
		setHasDerived(getFilteredDerivation() > 0);
	}
}
