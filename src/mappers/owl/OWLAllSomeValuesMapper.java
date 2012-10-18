package mappers.owl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;
import readers.MultiFilesReader;
import utils.NumberUtils;
import utils.TriplesUtils;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;

import data.Tree.ByteResourceNode;
import data.Tree.Node;
import data.Tree.Node.Rule;
import data.Tree.ResourceNode;
import data.Tree.TwoResourcesNode;
import data.Triple;
import data.TripleSource;

public class OWLAllSomeValuesMapper
		extends
		Mapper<TripleSource, Triple, BytesWritable, ProtobufWritable<ByteResourceNode>> {

	private static Logger log = LoggerFactory
			.getLogger(OWLAllSomeValuesMapper.class);

	private byte[] bKey = new byte[17];
	private BytesWritable oKey = new BytesWritable(bKey);

	private ProtobufWritable<ByteResourceNode> oValueContainer = ProtobufWritable
			.newInstance(ByteResourceNode.class);
	private Node.Builder builderNode = Node.newBuilder();
	protected ByteResourceNode.Builder oValue = ByteResourceNode.newBuilder();

	private static Map<Long, Collection<TwoResourcesNode>> allValues = null;
	private static Map<Long, Collection<TwoResourcesNode>> someValues = null;
	private static Set<Long> onPropertyAll = null;
	private static Set<Long> onPropertySome = null;

	@Override
	public void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {

		if (value.getPredicate() == TriplesUtils.RDF_TYPE) {

			if (someValues.containsKey(value.getObject())) {
				bKey[0] = 2;
				oValue.setId(1);
				NumberUtils.encodeLong(bKey, 9, value.getSubject());
				Collection<TwoResourcesNode> values = someValues.get(value
						.getObject());
				for (TwoResourcesNode trn : values) {
					NumberUtils.encodeLong(bKey, 1, trn.getResource1());
					oValue.setResource(trn.getResource2());

					builderNode.setRule(Rule.OWL_SOME_VALUES);
					builderNode.setStep(Math.max(trn.getHistory().getStep(),
							key.getStep()));
					builderNode.clearChildren();
					builderNode.addChildren(key.getHistory());
					builderNode.addAllChildren(trn.getHistory()
							.getChildrenList());
					oValue.setHistory(builderNode.build());

					oValueContainer.set(oValue.build());
					context.write(oKey, oValueContainer);
				}
			}

			if (allValues.containsKey(value.getObject())) {
				bKey[0] = 1;
				oValue.setId(1);
				NumberUtils.encodeLong(bKey, 9, value.getSubject());
				Collection<TwoResourcesNode> values = allValues.get(value
						.getObject());
				for (TwoResourcesNode trn : values) {
					NumberUtils.encodeLong(bKey, 1, trn.getResource1());
					oValue.setResource(trn.getResource2());
					builderNode.setRule(Rule.OWL_ALL_VALUES);
					builderNode.setStep(Math.max(trn.getHistory().getStep(),
							key.getStep()));
					builderNode.clearChildren();
					builderNode.addChildren(key.getHistory());
					builderNode.addAllChildren(trn.getHistory()
							.getChildrenList());
					oValue.setHistory(builderNode.build());
					oValueContainer.set(oValue.build());
					context.write(oKey, oValueContainer);
				}
			}

		} else {

			if (onPropertySome.contains(value.getPredicate())) {
				// Rule 15 - someValuesFrom
				bKey[0] = 2;
				oValue.setId(0);
				NumberUtils.encodeLong(bKey, 1, value.getPredicate());
				NumberUtils.encodeLong(bKey, 9, value.getObject());
				oValue.setResource(value.getSubject());
				oValue.setHistory(key.getHistory());
				oValueContainer.set(oValue.build());
				context.write(oKey, oValueContainer);
			}

			if (onPropertyAll.contains(value.getPredicate())) {
				// Rule 16 - allValuesFrom
				bKey[0] = 1;
				oValue.setId(0);
				NumberUtils.encodeLong(bKey, 1, value.getPredicate());
				NumberUtils.encodeLong(bKey, 9, value.getSubject());
				oValue.setResource(value.getObject());
				oValue.setHistory(key.getHistory());
				oValueContainer.set(oValue.build());
				context.write(oKey, oValueContainer);
			}
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		List<FileStatus> filesProperty = MultiFilesReader.recursiveListStatus(
				context, "FILTER_ONLY_OWL_ON_PROPERTY");

		Map<Long, Collection<ResourceNode>> allValuesTmp = FilesTriplesReader
				.loadCompleteMapIntoMemoryWithInfo(
						"FILTER_ONLY_OWL_ALL_VALUES", context, false);
		Map<Long, Collection<ResourceNode>> someValuesTmp = FilesTriplesReader
				.loadCompleteMapIntoMemoryWithInfo(
						"FILTER_ONLY_OWL_SOME_VALUES", context, false);

		onPropertyAll = new HashSet<Long>();
		onPropertySome = new HashSet<Long>();
		someValues = new HashMap<Long, Collection<TwoResourcesNode>>();
		allValues = new HashMap<Long, Collection<TwoResourcesNode>>();
		makeJoin(filesProperty.toArray(new FileStatus[filesProperty.size()]),
				context, someValuesTmp, allValuesTmp, someValues, allValues,
				onPropertySome, onPropertyAll);
	}

	protected void makeJoin(FileStatus[] files, Context context,
			Map<Long, Collection<ResourceNode>> someValuesTmp,
			Map<Long, Collection<ResourceNode>> allValuesTmp,
			Map<Long, Collection<TwoResourcesNode>> someValues,
			Map<Long, Collection<TwoResourcesNode>> allValues,
			Set<Long> onPropertySome, Set<Long> onPropertyAll) {
		TripleSource key = new TripleSource();
		Triple value = new Triple();

		TwoResourcesNode.Builder builder = TwoResourcesNode.newBuilder();
		Node.Builder builderSome = Node.newBuilder();
		builderSome.setRule(Rule.OWL_SOME_VALUES);

		Node.Builder builderAll = Node.newBuilder();
		builderAll.setRule(Rule.OWL_ALL_VALUES);

		for (FileStatus file : files) {
			SequenceFile.Reader input = null;
			FileSystem fs = null;
			try {
				fs = file.getPath().getFileSystem(context.getConfiguration());
				input = new SequenceFile.Reader(fs, file.getPath(),
						context.getConfiguration());
				boolean nextTriple = false;
				do {
					nextTriple = input.next(key, value);
					if (nextTriple) {
						// Check if there is a match with someValuesFrom and
						// allValuesFrom
						if (someValuesTmp.containsKey(value.getSubject())) {
							Collection<ResourceNode> col = someValuesTmp
									.get(value.getSubject());
							if (col != null) {
								Iterator<ResourceNode> itr = col.iterator();
								while (itr.hasNext()) {
									ResourceNode w_step = itr.next();
									long w = w_step.getResource();
									int step = w_step.getStep();

									builderSome.clearChildren();
									builderSome.addChildren(key.getHistory());
									builderSome
											.addChildren(w_step.getHistory());
									builderSome.setStep(Math.max(step,
											key.getStep()));

									builder.setResource1(value.getObject());
									builder.setResource2(value.getSubject());
									builder.setHistory(builderSome.build());

									Collection<TwoResourcesNode> cValues = someValues
											.get(w);
									if (cValues == null) {
										cValues = new ArrayList<TwoResourcesNode>();
										someValues.put(w, cValues);
									}
									cValues.add(builder.build());
									onPropertySome.add(value.getObject());
								}
							}
						}

						if (allValuesTmp.containsKey(value.getSubject())) {
							Collection<ResourceNode> col = allValuesTmp
									.get(value.getSubject());
							if (col != null) {
								Iterator<ResourceNode> itr = col.iterator();
								while (itr.hasNext()) {
									ResourceNode w_step = itr.next();
									long w = w_step.getResource();
									int step = w_step.getStep();

									builderAll.clearChildren();
									builderAll.addChildren(key.getHistory());
									builderAll.addChildren(w_step.getHistory());
									builderAll.setStep(Math.max(step,
											key.getStep()));

									builder.setResource1(value.getObject());
									builder.setResource2(w);
									builder.setHistory(builderAll.build());

									Collection<TwoResourcesNode> cValues = allValues
											.get(value.getSubject());
									if (cValues == null) {
										cValues = new ArrayList<TwoResourcesNode>();
										allValues.put(value.getSubject(),
												cValues);
									}

									cValues.add(builder.build());
									onPropertyAll.add(value.getObject());
								}
							}
						}
					}
				} while (nextTriple);

			} catch (IOException e) {
				log.error("Failed reading schema files", e);
			} finally {
				try {
					if (input != null) {
						input.close();
					}
				} catch (IOException e) {
					log.error("Failed in closing the input stream");
				}
			}
		}
	}
}
