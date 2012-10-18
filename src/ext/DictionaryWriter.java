package ext;

import java.util.ArrayList;

import data.Triple;

public class DictionaryWriter {

	/***** Standard URIs IDs *****/
	public static final long RDF_TYPE = 0;

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