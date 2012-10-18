package jobs;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.vu.cs.querypie.storage.disk.TripleFile;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class ConvertPlainBinaryFilesToWebPIE extends Configured implements Tool {

	private Logger log = LoggerFactory
			.getLogger(ConvertPlainBinaryFilesToWebPIE.class);
	String sFilter = null;
	String sClassName = null;

	public void parseArgs(String[] args) {
		for (int i = 0; i < args.length; ++i) {

			if (args[i].equalsIgnoreCase("--prefix")) {
				sFilter = args[++i];
			}

			if (args[i].equalsIgnoreCase("--inputType")) {
				sClassName = args[++i];
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Usage: ConvertPlainBinaryFilesToWebPIE [input dir] [output dir] --prefix PREFIX FILES --inputType NAME_INPUT_CLASS");
			System.exit(0);
		}

		new ConvertPlainBinaryFilesToWebPIE().run(args);
	}

	private List<File> readFiles(File file, String prefix) {
		List<File> list = new ArrayList<File>();

		if (file.isDirectory()) {
			// List all children and process each of them
			for (File child : file.listFiles())
				list.addAll(readFiles(child, prefix));

		} else if (file.getName().startsWith(prefix)) {
			list.add(file);
		}

		return list;
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgs(args);

		// Recursively read all files
		List<File> files = readFiles(new File(args[0]), sFilter);

		if (files == null) {
			System.out.println("No file is in " + args[0]
					+ " that matches the prefix " + sFilter);
			return 0;
		}

		int count = 0;
		Class<? extends TripleFile> indexFileImpl = ClassLoader
				.getSystemClassLoader().loadClass(sClassName)
				.asSubclass(TripleFile.class);
		Constructor<? extends TripleFile> constr = indexFileImpl
				.getConstructor(String.class);

		Map<Integer, SequenceFile.Writer> map = new HashMap<Integer, SequenceFile.Writer>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		TripleSource key = new TripleSource();
		key.setStep(0);
		Triple triple = new Triple();

		for (File file : files) {
			log.info("Process file " + file.getAbsolutePath());
			TripleFile reader = constr.newInstance(file.getPath());

			reader.open();
			while (reader.next()) {
				count++;

				int type = TriplesUtils.getTripleType(null,
						reader.getFirstTerm(), reader.getSecondTerm(),
						reader.getThirdTerm());

				triple.setSubject(reader.getFirstTerm());
				triple.setPredicate(reader.getSecondTerm());
				triple.setObject(reader.getThirdTerm());

				SequenceFile.Writer writer = map.get(type);
				if (writer == null) {
					String name = TriplesUtils.getFileExtensionByTripleType(
							key, triple, "update");
					Path path = new Path(args[1], name);
					writer = new SequenceFile.Writer(fs, conf, path,
							TripleSource.class, Triple.class);
					map.put(type, writer);
				}

				writer.append(key, triple);
			}
			reader.close();
		}

		// Closes all the files
		for (Map.Entry<Integer, SequenceFile.Writer> entry : map.entrySet()) {
			entry.getValue().close();
		}

		log.info("Converted triples " + count);
		return 0;
	}
}
