package mappers.rdfs;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import readers.FilesTriplesReader;

import utils.NumberUtils;
import utils.TriplesUtils;
import data.Triple;
import data.TripleSource;

public class RDFSSpecialPropsMapper extends Mapper<TripleSource, Triple, BytesWritable, LongWritable> {
	
	protected static Logger log = LoggerFactory.getLogger(RDFSSpecialPropsMapper.class);

	protected LongWritable oValue = new LongWritable(0);
	byte[] bKey = new byte[17];
	protected BytesWritable oKey = new BytesWritable();
	
	protected Set<Long> memberProperties = null;
	protected Set<Long> resourceSubclasses = null;
	protected Set<Long> literalSubclasses = null;
	
	public void map(TripleSource key, Triple value, Context context) throws IOException, InterruptedException {
		if (value.getPredicate() == TriplesUtils.RDF_TYPE) {
			if ((value.getObject() == TriplesUtils.RDFS_LITERAL || 
					literalSubclasses.contains(value.getObject()))) {
				bKey[0] = 0;
				NumberUtils.encodeLong(bKey, 1, value.getSubject());
				oKey.set(bKey, 0, 9);
				oValue.set(value.getObject());
				context.write(oKey, oValue);
			}
		} else if (value.getPredicate() == TriplesUtils.RDFS_SUBPROPERTY) {
			if ((value.getObject() == TriplesUtils.RDFS_MEMBER || 
						memberProperties.contains(value.getObject()))) {
				bKey[0] = 1;
				NumberUtils.encodeLong(bKey, 1, value.getSubject());
				oKey.set(bKey, 0, 9);
				oValue.set(value.getObject());
				context.write(oKey, oValue);
			}
		} else if (value.getPredicate() == TriplesUtils.RDFS_SUBCLASS) {
			if ((value.getObject() == TriplesUtils.RDFS_LITERAL || 
						literalSubclasses.contains(value.getObject()))) {
				bKey[0] = 2;
				NumberUtils.encodeLong(bKey, 1, value.getSubject());
				oKey.set(bKey, 0, 9);
				oValue.set(value.getObject());
				context.write(oKey, oValue);
			}
		} else if (memberProperties.contains(value.getPredicate()) ||
					value.getPredicate() == TriplesUtils.RDFS_MEMBER) {
			if (!value.isObjectLiteral())
				bKey[0] = 4;
			else
				bKey[0] = 5;
			NumberUtils.encodeLong(bKey, 1, value.getSubject());
			NumberUtils.encodeLong(bKey, 9, value.getObject());
			oKey.set(bKey, 0, 17);
			oValue.set(value.getPredicate());
			context.write(oKey, oValue);
		}
	}

	@Override
	public void setup(Context context) throws IOException {

		if (memberProperties == null) {
			memberProperties = new HashSet<Long>();
			FilesTriplesReader.loadSetIntoMemory(memberProperties, context, "FILTER_ONLY_MEMBER_SUBPROP_SCHEMA", -1);
		}
		
		if (resourceSubclasses == null) {
			resourceSubclasses = new HashSet<Long>();
			FilesTriplesReader.loadSetIntoMemory(resourceSubclasses, context, "FILTER_ONLY_RESOURCE_SUBCLAS_SCHEMA", -1);
		}
		
		if (literalSubclasses == null) {
			literalSubclasses = new HashSet<Long>();
			FilesTriplesReader.loadSetIntoMemory(literalSubclasses, context, "FILTER_ONLY_LITERAL_SUBCLAS_SCHEMA", -1);
		}
	}
}
