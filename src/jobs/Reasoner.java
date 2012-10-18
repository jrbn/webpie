package jobs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jobs.blocks.DeleteDuplicatesBlock;
import jobs.blocks.ExecutionBlock;
import jobs.blocks.OWLEquivalenceBlock;
import jobs.blocks.OWLNotRecursiveBlock;
import jobs.blocks.OWLSameAsReplacementBlock;
import jobs.blocks.OWLSameAsTransitivityBlock;
import jobs.blocks.OWLSomeAllValuesBlock;
import jobs.blocks.OWLTransitivityBlock;
import jobs.blocks.OWLhasValueBlock;
import jobs.blocks.RDFSDomainRangeBlock;
import jobs.blocks.RDFSSpecialCasesBlock;
import jobs.blocks.RDFSSubclassBlock;
import jobs.blocks.RDFSSubpropertiesBlock;
import jobs.strategies.FixedStrategy;
import jobs.strategies.GreedyStrategy;
import jobs.strategies.LazyStrategy;
import jobs.strategies.Strategy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reasoner extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(Reasoner.class);
	final public static String OWL_SYNONYMS_TABLE = "dir-table-synonyms";

	private Path pool = null;
	private int numMapTasks = 4;
	private int numReduceTasks = 2;
	private int sampling = 10;
	private int resourceThreshold = 1000;
	private int startFromStep = 0;

	Strategy strategy = new FixedStrategy();
	private int strategyDuplicates = ExecutionBlock.STRATEGY_CLEAN_DUPL_ALWAYS;
	private int derivationThreshold = 0;
	private int fragment = ExecutionBlock.FRAGMENT_RDFS;

	private void parseArgs(String[] args) {

		pool = new Path(args[0]);

		for (int i = 0; i < args.length; ++i) {
			if (args[i].equalsIgnoreCase("--maptasks")) {
				numMapTasks = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--samplingPercentage")) {
				sampling = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--samplingThreshold")) {
				resourceThreshold = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--lastStep")) {
				startFromStep = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--rulesStrategy")) {
				++i;
				if (args[i].equalsIgnoreCase("fixed")) {
					strategy = new FixedStrategy();
				} else if (args[i].equalsIgnoreCase("lazy")) {
					strategy = new LazyStrategy();
				} else if (args[i].equalsIgnoreCase("greedy")) {
					strategy = new GreedyStrategy();
				}
			}

			if (args[i].equalsIgnoreCase("--duplicatesStrategyThreshold")) {
				derivationThreshold = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--duplicatesStrategy")) {
				++i;
				if (args[i].equalsIgnoreCase("always")) {
					strategyDuplicates = ExecutionBlock.STRATEGY_CLEAN_DUPL_ALWAYS;
				} else if (args[i].equalsIgnoreCase("large")) {
					strategyDuplicates = ExecutionBlock.STRATEGY_CLEAN_DUPL_LARGE_DERIVATION;
				} else if (args[i].equalsIgnoreCase("end")) {
					strategyDuplicates = ExecutionBlock.STRATEGY_CLEAN_DUPL_END;
				}
			}

			if (args[i].equalsIgnoreCase("--fragment")) {
				++i;
				if (args[i].equalsIgnoreCase("rdfs")) {
					fragment = ExecutionBlock.FRAGMENT_RDFS;
				} else if (args[i].equalsIgnoreCase("owl")) {
					fragment = ExecutionBlock.FRAGMENT_OWL;
				} else if (args[i].equalsIgnoreCase("rdfsowl")) {
					fragment = ExecutionBlock.FRAGMENT_RDFSOWL;
				} else if (args[i].equalsIgnoreCase("owl2")) {
					fragment = ExecutionBlock.FRAGMENT_OWL2;
				} else if (args[i].equalsIgnoreCase("owl-sameas")) {
					fragment = ExecutionBlock.FRAGMENT_ONLY_OWL_SAMEAS;
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);

		// Choose fragment to consider. Could be RDFS, OWL or RDFS+OWL.
		Configuration conf = new Configuration(getConf());
		List<ExecutionBlock> blocksToExecute = new ArrayList<ExecutionBlock>();

		if (fragment == ExecutionBlock.FRAGMENT_RDFS
				|| fragment == ExecutionBlock.FRAGMENT_OWL) {
			ExecutionBlock block = new RDFSSubpropertiesBlock();
			blocksToExecute.add(block);

			block = new RDFSDomainRangeBlock();
			blocksToExecute.add(block);

			block = new RDFSSubclassBlock();
			blocksToExecute.add(block);

			block = new RDFSSpecialCasesBlock();
			blocksToExecute.add(block);
		}

		if (fragment == ExecutionBlock.FRAGMENT_OWL
				|| fragment == ExecutionBlock.FRAGMENT_OWL) {
			ExecutionBlock block = new OWLNotRecursiveBlock();
			blocksToExecute.add(block);

			block = new OWLTransitivityBlock();
			blocksToExecute.add(block);

			block = new OWLSameAsTransitivityBlock();
			blocksToExecute.add(block);

			block = new OWLSameAsReplacementBlock();
			blocksToExecute.add(block);

			block = new OWLEquivalenceBlock();
			blocksToExecute.add(block);

			block = new OWLhasValueBlock();
			blocksToExecute.add(block);

			block = new OWLSomeAllValuesBlock();
			blocksToExecute.add(block);
		}

		if (fragment == ExecutionBlock.FRAGMENT_ONLY_OWL_SAMEAS) {
			ExecutionBlock block = new OWLSameAsTransitivityBlock();
			blocksToExecute.add(block);

			block = new OWLSameAsReplacementBlock();
			blocksToExecute.add(block);
		}

		/*
		 * if (fragment == ExecutionBlock.FRAGMENT_OWL2) {
		 * blocksToExecute.add(new OWL2BuildLists()); blocksToExecute.add(new
		 * OWL2PropertyChainAxiom()); blocksToExecute.add(new OWL2HasKey());
		 * //blocksToExecute.add(new OWL2IntersectionOf()); }
		 */

		Iterator<ExecutionBlock> itr = blocksToExecute.iterator();
		while (itr.hasNext()) {
			itr.next().init(conf, pool, strategyDuplicates,
					derivationThreshold, numMapTasks, numReduceTasks, sampling,
					resourceThreshold, startFromStep);
		}

		strategy.setRuleset(blocksToExecute);

		long filteredDerivation = 0;
		long notFilteredDerivation = 0;
		int step = 0;
		int filterFromStep = 0;
		step = startFromStep;
		filterFromStep = startFromStep;

		long time = System.currentTimeMillis();
		while (strategy != null && strategy.shouldExecuteMoreRules()) {
			ExecutionBlock block = strategy.getNextRule();
			block.execute(++step, filterFromStep);

			filterFromStep = block.getFilterFromStep();
			filteredDerivation += block.getFilteredDerivation();
			notFilteredDerivation += block.getNotFilteredDerivation();

			strategy.updateStrategy();
		}

		/*
		 * if (sameAsReplacement != null &&
		 * conf.getBoolean("ShouldRunSameAsInheritance", false)) {
		 * sameAsReplacement.execute(++step, filterFromStep); filterFromStep =
		 * sameAsReplacement.getFilterFromStep(); }
		 */

		// This block is executed only if the duplicates strategy is different
		// than always
		ExecutionBlock cleanupBlock = new DeleteDuplicatesBlock();
		cleanupBlock.init(conf, pool, strategyDuplicates, derivationThreshold,
				numMapTasks, numReduceTasks, sampling, resourceThreshold,
				startFromStep);
		cleanupBlock.execute(++step, filterFromStep);
		filteredDerivation += cleanupBlock.getFilteredDerivation();

		log.info("Last step: " + step);
		log.info("Time derivation: " + (System.currentTimeMillis() - time));
		log.info("Number of derived triples: (filtered) " + filteredDerivation
				+ " (not filtered) " + notFilteredDerivation);

		return 0;
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("USAGE: Reasoner [pool] [options]");
			return;
		}

		try {
			ToolRunner.run(new Configuration(), new Reasoner(), args);
		} catch (Exception e) {
			log.error("Error in the execution of the reasoner", e);
		}
	}
}