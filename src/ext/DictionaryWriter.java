package ext;

import java.util.ArrayList;

import data.Triple;

public class DictionaryWriter {

	/***** Standard URIs IDs *****/
	public static final long RDF_TYPE = 0;
	public static final long RDF_PROPERTY = 1;
	public static final long RDFS_RANGE = 2;
	public static final long RDFS_DOMAIN = 3;
	public static final long RDFS_SUBPROPERTY = 4;
	public static final long RDFS_SUBCLASS = 5;
	public static final long RDFS_MEMBER = 19;
	public static final long RDFS_LITERAL = 20;
	public static final long RDFS_CONTAINER_MEMBERSHIP_PROPERTY = 21;
	public static final long RDFS_DATATYPE = 22;
	public static final long RDFS_CLASS = 23;
	public static final long RDFS_RESOURCE = 24;
	public static final long OWL_CLASS = 6;
	public static final long OWL_FUNCTIONAL_PROPERTY = 7;
	public static final long OWL_INVERSE_FUNCTIONAL_PROPERTY = 8;
	public static final long OWL_SYMMETRIC_PROPERTY = 9;
	public static final long OWL_TRANSITIVE_PROPERTY = 10;
	public static final long OWL_SAME_AS = 11;
	public static final long OWL_INVERSE_OF = 12;
	public static final long OWL_EQUIVALENT_CLASS = 13;
	public static final long OWL_EQUIVALENT_PROPERTY = 14;
	public static final long OWL_HAS_VALUE = 15;
	public static final long OWL_ON_PROPERTY = 16;
	public static final long OWL_SOME_VALUES_FROM = 17;
	public static final long OWL_ALL_VALUES_FROM = 18;

	long currentS = -1;

	public void startSection(int classId, long id) {
		currentS = id;
		writeTriple(id, RDF_TYPE, classId + 128); // Classes of LUBM start at
													// 128
	}

	public void addProperty(int predicate, long object, boolean isResource) {
		if (!isResource) {
			object |= 1; // object is literal
		}
		writeTriple(currentS, predicate + 64, object);
		// if (isResource)
		// writeTriple(object, RDF_TYPE, RDFS_RESOURCE);
	}

	public void addProperty(int predicate, int objectType, long objectID) {
		writeTriple(objectID, RDF_TYPE, objectType + 128);
		writeTriple(currentS, predicate + 64, objectID);
	}

	public void endSection(int classId) {
		currentS = -1;

	}

	public void startAboutSection(int classID, long id) {
		startSection(classID, id);
	}

	private void writeTriple(long s, long p, long o) {
		boolean oIsLiteral = ((o >= 512) && (o & 1) == 1);
		data.add(new Triple(s, p, o, oIsLiteral));
		// System.out.println(s+ " " + p + " " + o);
	}

	private ArrayList<Triple> data = new ArrayList<Triple>(200000);

	public ArrayList getData() {
		return data;
	}

}