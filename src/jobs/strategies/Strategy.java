package jobs.strategies;

import java.util.List;

import jobs.blocks.ExecutionBlock;

public interface Strategy {

	public ExecutionBlock getNextRule();
	public boolean shouldExecuteMoreRules();
	public void setRuleset(List<ExecutionBlock> rules);
	public void updateStrategy();
}
