package mappers.io;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CheckConsistencyCompressionMapper extends
		Mapper<Text, Text, Text, NullWritable> {

	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		if (!value.toString().equalsIgnoreCase(""))
			context.write(value, NullWritable.get());
	}

}