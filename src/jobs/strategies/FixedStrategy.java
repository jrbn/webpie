package jobs.strategies;

import java.util.Iterator;
import java.util.List;

import jobs.blocks.ExecutionBlock;

public class FixedStrategy implements Strategy {
	
	List<ExecutionBlock> rules = null;
	Iterator<ExecutionBlock> itr = null;
	int countRulesWithNoDerivation = 0;
	ExecutionBlock block = null;

	@Override
	public ExecutionBlock getNextRule() {
		if (!itr.hasNext()) { //Start from the beginning
			itr = rules.iterator();
		}
		block = itr.next();
		return block;
	}

	@Override
	public void setRuleset(List<ExecutionBlock> rules) {
		this.rules = rules;
		itr = rules.iterator();
	}

	@Override
	public boolean shouldExecuteMoreRules() {
		return countRulesWithNoDerivation < rules.size();
	}

	@Override
	public void updateStrategy() {
		if (!block.hasDerived()) {
			countRulesWithNoDerivation++;
		} else {
			countRulesWithNoDerivation = 0;
		}
	}
}
