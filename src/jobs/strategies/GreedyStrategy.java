package jobs.strategies;

import java.util.LinkedList;
import java.util.List;

import jobs.blocks.ExecutionBlock;

public class GreedyStrategy implements Strategy {

	LinkedList<ExecutionBlock> rules = new LinkedList<ExecutionBlock>();
	int countRulesWithNoDerivation = 0;

	@Override
	public ExecutionBlock getNextRule() {
		return rules.get(0);
	}

	@Override
	public void setRuleset(List<ExecutionBlock> rules) {
		this.rules.addAll(rules);
	}

	@Override
	public boolean shouldExecuteMoreRules() {
		return countRulesWithNoDerivation < rules.size();
	}

	@Override
	public void updateStrategy() {
		ExecutionBlock rule = rules.get(0);
		rules.remove(0);

		if (!rule.hasDerived()) {
			countRulesWithNoDerivation++;
		} else {
			countRulesWithNoDerivation = 0;
		}

		if (!rule.hasDerived() && !rules.get(0).hasDerived()) {
			rules.add(rule);
		} else {
			int i = 0;
			while (i < rules.size()) {
				ExecutionBlock currentRule = rules.get(i);
				if (!currentRule.isNeverExecuted()) {
					long currentDerivation = currentRule
							.getFilteredDerivation() > 0 ? currentRule
							.getFilteredDerivation() : currentRule
							.getNotFilteredDerivation();
					long derivation = rule.getFilteredDerivation() > 0 ? rule
							.getFilteredDerivation() : rule
							.getNotFilteredDerivation();
					if (derivation > currentDerivation) {
						rules.add(i, rule);
						return;
					}
				}
				++i;
			}
			rules.add(rule);
		}
	}
}
