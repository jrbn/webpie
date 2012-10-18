package jobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import mappers.io.ExtractNumberDictionaryMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractNumberDictionary extends Configured implements Tool {

    protected static Logger log = LoggerFactory
	    .getLogger(ExtractNumberDictionary.class);

    public static void main(String[] args) throws Exception {

	if (args.length < 2) {
	    System.out
		    .println("Usage: ExtractNumberDictionary [dict dir] [output dir] --url URL --queries <dir with SPARQL queries>");
	    System.exit(0);
	}

	int res = ToolRunner.run(new Configuration(),
		new ExtractNumberDictionary(), args);
	System.exit(res);
    }

    String url = null;
    String queries = null;

    @Override
    public int run(String[] args) throws Exception {

	for (int i = 0; i < args.length; ++i) {
	    if (args[i].equalsIgnoreCase("--url")) {
		url = args[++i];
	    }

	    if (args[i].equalsIgnoreCase("--queries")) {
		queries = args[++i];
	    }
	}

	Job job = new Job();
	job.setJobName("Extract number from dictionary");
	job.setJarByClass(ExtractNumberDictionary.class);

	job.setInputFormatClass(SequenceFileInputFormat.class);
	FileInputFormat.setInputPathFilter(job, OutputLogFilter.class);
	SequenceFileInputFormat.addInputPath(job, new Path(args[0], "0"));
	SequenceFileInputFormat.addInputPath(job, new Path(args[0], "1"));
	job.setMapperClass(ExtractNumberDictionaryMapper.class);
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(Text.class);

	String toSet = "";
	if (url != null) {
	    toSet = url;
	} else if (queries != null) {
	    try {
		File file = new File(queries);
		List<File> files = new ArrayList<File>();
		if (file.isDirectory()) {
		    for (File f : file.listFiles()) {
			files.add(f);
		    }
		} else {
		    files.add(file);
		}

		HashSet<String> strings = new HashSet<String>();
		for (File f : files) {
		    try {
			BufferedReader r = new BufferedReader(new FileReader(f));
			String query = "";
			String line = "";
			while ((line = r.readLine()) != null) {
			    query += "\n" + line;
			}

			// Parse the query and return the URLs
			SPARQLParser parser = new SPARQLParser();
			ParsedQuery q = parser.parseQuery(query,
				"http://www.vu.nl/");

			TupleExpr expr = q.getTupleExpr();

			final List<StatementPattern> list = new ArrayList<StatementPattern>();
			expr.visit(new QueryModelVisitorBase<RuntimeException>() {
			    @Override
			    public void meet(StatementPattern node)
				    throws RuntimeException {
				list.add(node);
			    }
			});

			// Replace URIs with the numbers from the dictionary
			// table
			for (StatementPattern sp : list) {
			    for (Var var : sp.getVarList()) {
				if (var.getValue() != null) {
				    String value = var.getValue().stringValue();
				    if (var.getValue() instanceof LiteralImpl) {
					value = "\"" + value + "\"";
				    } else {
					value = "<" + value + ">";
				    }
				    strings.add(value);
				}
			    }
			}

			r.close();
		    } catch (Exception e) {
			log.warn("Failed parsing file " + f.getName());
		    }
		}

		log.info("Going  to retrieve " + strings.size()
			+ " for the URIs=" + strings);
		Iterator<String> itr = strings.iterator();
		if (itr.hasNext()) {
		    toSet = itr.next();
		}

		while (itr.hasNext()) {
		    toSet += "," + itr.next();
		}

	    } catch (Exception e) {
		log.error("Error", e);
	    }
	} else {
	    log.error("No URL is specified (either --url or --queries)");
	    System.exit(1);
	}

	job.getConfiguration().set("uri", toSet);
	job.setNumReduceTasks(0);
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(Text.class);
	TextOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setOutputFormatClass(TextOutputFormat.class);
	job.waitForCompletion(true);

	return 0;
    }
}
