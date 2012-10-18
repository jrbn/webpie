package jobs;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.MultiFilesReader;
import nl.vu.cs.querypie.storage.disk.TripleFile;
import utils.NumberUtils;
import data.Triple;
import data.TripleSource;

public class ConvertFilesToPlainBinary extends Configured implements Tool {

	private Logger log = LoggerFactory
			.getLogger(ConvertFilesToPlainBinary.class);
	String sFilter = null;
	String sClassName = null;

	public void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {

			if (args[i].equalsIgnoreCase("--filter")) {
				sFilter = args[++i];
			}

			if (args[i].equalsIgnoreCase("--outputType")) {
				sClassName = args[++i];
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Usage: ConvertFilesToPlainBinary [input dir] [output dir] --filter NAME_FILTER --outputType NAME_OUTPUT_CLASS");
			System.exit(0);
		}

		new ConvertFilesToPlainBinary().run(args);
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);

		FileSystem fs = FileSystem.get(new Configuration());
		Collection<FileStatus> files = MultiFilesReader.recursiveListStatus(
				new Configuration(), new Path(args[0]), sFilter);

		Class<? extends TripleFile> indexFileImpl = ClassLoader
				.getSystemClassLoader().loadClass(sClassName)
				.asSubclass(TripleFile.class);
		Constructor<? extends TripleFile> constr = indexFileImpl
				.getConstructor(String.class);

		TripleFile of = null;

		int count = 0;
		for (FileStatus file : files) {
			SequenceFile.Reader input = new SequenceFile.Reader(fs,
					file.getPath(), new Configuration());
			Writable key = input.getKeyClass().asSubclass(Writable.class)
					.newInstance();
			log.debug("Process file " + file.getPath());
			long triplesFile = 0;
			while (input.next(key)) {

				if (of == null) {
					String outputFile = args[1] + File.separator
							+ "input-triples";
					of = constr.newInstance(outputFile);
					of.openToWrite();
				}

				count++;
				triplesFile++;
				
				byte[] tValue = null;
				if (key instanceof TripleSource) {
					Writable value = input.getValueClass()
							.asSubclass(Writable.class).newInstance();
					input.getCurrentValue(value);
					Triple triple = (Triple) value;
					tValue = new byte[24];
					NumberUtils.encodeLong(tValue, 0, triple.getSubject());
					NumberUtils.encodeLong(tValue, 8, triple.getPredicate());
					NumberUtils.encodeLong(tValue, 16, triple.getObject());
				} else {
					tValue = Arrays
							.copyOf(((BytesWritable) key).getBytes(), 24);
				}
				of.writeTriple(tValue, 24);
			}
			log.debug("Triples: " + triplesFile);
		}

		if (of != null)
			of.close();
		log.info("Converted triples " + count);
		return 0;
	}
}
