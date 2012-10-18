package mappers.io;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.Triple;
import ext.Generator;

public class CreateLUBMDatasetMapper extends
		Mapper<LongWritable, Text, Triple, NullWritable> {

	private int startingUniversity = 0;
	private int currentUniversity = 0;
	private int numberUniversity = 0;
	protected static Logger log = LoggerFactory.getLogger(CreateLUBMDatasetMapper.class);

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//log.debug("map code: startingUniversity = " + startingUniversity + " currentUniversity = " + currentUniversity);
		if (startingUniversity == 0 && currentUniversity == 0) {
			Collection<Triple> schema=Generator.getSchema();
			Iterator<Triple> itr = schema.iterator();
			while (itr.hasNext()) {
				context.write(itr.next(), NullWritable.get());
			}
			
			//Collection<Triple> axioms=Generator.getAxiomaticTriples();
			//itr = axioms.iterator();
			//while (itr.hasNext()) {
				//output.collect(itr.next(), NullWritable.get());
			//}
		}
		
		//log.debug("currentUniversity = " + currentUniversity + " limit = " + (startingUniversity + numberUniversity));
		while (currentUniversity < (startingUniversity + numberUniversity)) {
			Generator g=new Generator(1, currentUniversity, 0); // 1 university, start at index 0, use 0 for random seed
			Collection<Triple> data=g.getData();
			//log.debug(" data size = " + data.size());
			Iterator<Triple> itr = data.iterator();
			while (itr.hasNext()) {
				Triple triple = itr.next();
				context.write(triple, NullWritable.get());
			}
			++currentUniversity;
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		numberUniversity = context.getConfiguration().getInt("uniPerTask", -1);
		String taskId = context.getConfiguration().get("mapred.task.id").substring(context.getConfiguration().get("mapred.task.id").indexOf("_m_") + 3);
		taskId = taskId.substring(0, taskId.indexOf('_'));
		startingUniversity = Integer.valueOf(taskId) * numberUniversity;
		currentUniversity = startingUniversity;
		
		log.debug("starting university " + startingUniversity);
		log.debug("number university " + numberUniversity);
	}

}