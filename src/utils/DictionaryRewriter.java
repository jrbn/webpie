package utils;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DictionaryRewriter {

	static Logger log = LoggerFactory.getLogger(DictionaryRewriter.class);

	private static void rewrite(String inputDir, String outputDir) {
		try {
			
			Configuration conf = new Configuration();
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] f1 = fs.listStatus(new Path(inputDir,
					"_dict/table/0"), new OutputLogFilter());
			FileStatus[] f2 = fs.listStatus(new Path(inputDir,
					"_dict/table/1"), new OutputLogFilter());
			
			FileStatus[] files = new FileStatus[f1.length + f2.length];
			System.arraycopy(f1, 0, files, 0, f1.length);
			System.arraycopy(f2, 0, files, f1.length, f2.length);
			
			System.out.println("Size files " + files.length);
			
			LongWritable key = new LongWritable();
			BytesWritable value = new BytesWritable();
			
			log.debug("Files to load " + files.length);
			
			DataOutputStream indexOut = new DataOutputStream(
					new BufferedOutputStream(
				    new FileOutputStream(outputDir + File.separator + "index.data")));

			DataOutputStream textOut = new DataOutputStream(
					new BufferedOutputStream(
				    new FileOutputStream(outputDir + File.separator + "text.data")));

			long offset = 0;
			
			for (FileStatus file : files) {
				FileStatus[] tableFiles = fs.listStatus(file.getPath(), new OutputLogFilter());
				
				int i = 0;
				
				for (FileStatus tableEntry : tableFiles) {
				
					log.info("Rewrite file " + file.getPath().getName() + "("
							+ (tableFiles.length - i++) + ")");
					
					SequenceFile.Reader reader = new SequenceFile.Reader(fs,
							tableEntry.getPath(), conf);
					
					while (reader.next(key, value)) {
						
						long hash = key.get();
						int size = value.getLength();
						
						byte [] data = value.getBytes();
						
						indexOut.writeLong(hash);
						indexOut.writeLong(offset);
						indexOut.writeInt(size);
						
						textOut.write(data, 0, size);
					
						offset += size;
					}
				}
			}
			
			indexOut.close();
			textOut.close();
		
		} catch (Exception e) {
			log.error("Error in loading the dictionary", e);
		}
	}
	
	public static void main(String[] args) {
		
		// Load the dictionary and serve requests
		log.info("Loading dictionary ...");
		rewrite(args[0], args[1]);
		log.info("done ...");
	}
}
