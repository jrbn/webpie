package mappers.io;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import data.Tree.Node;
import data.Triple;
import data.TripleSource;

public class FlattenDerTreeMapper extends
		Mapper<TripleSource, Triple, NullWritable, Text> {

	Text oValue = new Text();

	@Override
	protected void map(TripleSource key, Triple value, Context context)
			throws IOException, InterruptedException {
		// Read the tree and print it
		Node node = key.getHistory();

		// bfs
		LinkedList<Node> list = new LinkedList<Node>();
		list.add(node);

		String output = "[";
		int nlayer = 1;
		int nlevels = 0;
		while (list.size() > 0) {
			nlayer--;
			node = list.remove();
			output+= "(" + node.getRule().name() + "," + node.getStep() + ") ";

			if (node.getChildrenCount() > 0) {
				list.addAll(node.getChildrenList());
			}

			if (nlayer == 0) {
				output = output.trim();
				output += "]";
				nlevels++;
				nlayer = list.size();
				if (nlayer > 0) {
					output += ",[";
				}
			}
		}

		oValue.set(nlevels + "\t" + output);
		context.write(NullWritable.get(), oValue);

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	}
}