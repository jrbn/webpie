package test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

import readers.FilesTriplesReader;
import data.Tree.Node.Rule;
import data.Tree.ResourceNode;

public class LoadSubclassTriples {

	public static void main(String[] args) throws IOException {
		System.out.println("Load triples in args[0]...");

		Configuration conf = new Configuration();
		conf.set("mapred.input.dir", args[0]);

		JobContext context = new JobContext(conf, new JobID());
		Map<Long, Collection<ResourceNode>> triples = FilesTriplesReader
				.loadCompleteMapIntoMemoryWithInfo(Rule.RDFS_SUBCLASS_TRANS,
						"FILTER_ONLY_SUBCLASS_SCHEMA", context, false);
		System.out.println(triples.size());
	}

}
