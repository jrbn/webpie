package ext;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import data.Triple;

public class DictionaryDecoder {
	public static void main(String[] args) throws Exception {
		new DictionaryDecoder(args[0]);
	}

	private Map<Long, String> reverseMappings = new HashMap<Long, String>();

	public DictionaryDecoder(String ontoFile) throws Exception {
		SimpleLUBMDict d = new SimpleLUBMDict();
		d.encode(ontoFile);

		Generator g = new Generator(1, 0, 0);

		Collection<Triple> encoded = g.getData();
		Collection<Triple> encodedSchema = g.getSchema();

		for (Map.Entry<String, Long> e : d.getMappings().entrySet()) {
			String previous = reverseMappings.put(e.getValue(), e.getKey());
			if (previous != null) {
				System.err.println("Found duplicate mapping: " + e.getKey()
						+ "->" + e.getValue());
			}
		}

		FileOutputStream f = new FileOutputStream("instanceData.nt");

		decode(encoded, f);
	}

	public void decode(Collection<Triple> c, OutputStream out)
			throws IOException {
		BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(out));

		for (Triple t : c) {
			wr.write(decodeTerm(t.getSubject()) + " ");
			wr.write(decodeTerm(t.getPredicate()) + " ");
			wr.write(decodeTerm(t.getObject()) + " .\n");
		}
		wr.flush();
	}

	public String decodeTerm(Long t) {
		String m = reverseMappings.get(t);
		if (m != null) {
			return m;
		}

		// Not a well-known id search generated ones
		return idToURI(t);
	}

	/*
	 * OLD 62: Flag for types with relative name 61: Flag for publications 60:
	 * Flag to indicate not well-known ids // Rooms for lots 30-59: University
	 * // Room for 1G universities 23-29: Department // Room for 64 departments
	 * 16-22: Classtype // Room for 64 types 8-15: Index of thing // Room for
	 * 128 1-7: Publication // Used only for publications 0: flag for literals
	 */

	/*
	 * NEW 62: Flag for types with relative name 61: Flag for publications 60:
	 * Flag to indicate not well-known ids // Rooms for lots 38-59: University
	 * // Room for 2M universities 31-37: Department // Room for 64 departments
	 * 24-30: Classtype // Room for 64 types 8-23: Index of thing // Room for
	 * 32768 1-7: Publication // Used only for publications 0: flag for literals
	 */

	private String idToURI(Long id) {
		if (id == Long.MAX_VALUE) {// telephone
			return "\"xxx-xxx-xxxx\"";
		}
		if (((id >> 60) & 1) == 0) {
			throw new IllegalStateException("id is well-known: " + id);
		}
		int classType = (int) ((id >> 24) & 63);
		int university = (int) ((id >> 38) & 2097151);
		int dept = (int) ((id >> 31) & 63);
		int thing = (int) ((id >> 8) & 32767);
		int publication = (int) ((id >> 1) & 63);
		boolean literal = (id & 1) == 1;
		boolean relativeName = (((id >> 62) & 1) == 1);

		String s;

		if (relativeName) {
			s = _getRelativeName(classType, thing);
			if (literal) {
				return "\"" + s + "\"";
			} else
				return "<" + s + ">";
		}

		if (literal) {
			return "\"" + _getEmail(classType, thing, university, dept) + "\"";
		}

		return "<" + _getId(classType, thing, university, dept, publication)
				+ ">";
	}

	/**
	 * Gets the id of the specified instance.
	 * 
	 * @param classType
	 *            Type of the instance.
	 * @param index
	 *            Index of the instance within its type.
	 * @param university
	 * @param dept
	 * @return Id of the instance.
	 */
	private String _getId(int classType, int index, int university, int dept,
			int publication) {
		String id;

		switch (classType) {
		case Generator.CS_C_UNIV:
			id = "http://www." + _getRelativeName(classType, university)
					+ ".edu";
			break;
		case Generator.CS_C_DEPT:
			id = "http://www." + _getRelativeName(classType, index) + "."
					+ _getRelativeName(Generator.CS_C_UNIV, university)
					+ ".edu";
			break;
		default:
			id = _getId(Generator.CS_C_DEPT, dept, university, dept, 0)
					+ Generator.ID_DELIMITER
					+ _getRelativeName(classType, index);
			break;

		}

		if (publication > 0) {
			id += Generator.ID_DELIMITER
					+ Generator.CLASS_TOKEN[Generator.CS_C_PUBLICATION]
					+ (publication - 1);
		}

		return id;
	}

	/**
	 * Gets the globally unique name of the specified instance.
	 * 
	 * @param classType
	 *            Type of the instance.
	 * @param index
	 *            Index of the instance within its type.
	 * @return Name of the instance.
	 */
	private String _getName(int classType, int index, int university, int dept) {
		String name;

		switch (classType) {
		case Generator.CS_C_UNIV:
			name = _getRelativeName(classType, index);
			break;
		case Generator.CS_C_DEPT:
			name = _getRelativeName(classType, index)
					+ Generator.INDEX_DELIMITER + "" + university;
			break;
		// NOTE: Assume departments with the same index share the same pool of
		// courses and researches
		case Generator.CS_C_COURSE:
		case Generator.CS_C_GRADCOURSE:
		case Generator.CS_C_RESEARCH:
			name = _getRelativeName(classType, index)
					+ Generator.INDEX_DELIMITER + "" + dept;
			break;
		default:
			name = _getRelativeName(classType, index)
					+ Generator.INDEX_DELIMITER + "" + dept
					+ Generator.INDEX_DELIMITER + "" + university;
			break;
		}

		return name;
	}

	/**
	 * Gets the name of the specified instance that is unique within a
	 * department.
	 * 
	 * @param classType
	 *            Type of the instance.
	 * @param index
	 *            Index of the instance within its type.
	 * @return Name of the instance.
	 */
	private String _getRelativeName(int classType, int index) {
		String name;

		switch (classType) {
		case Generator.CS_C_UNIV:
			// should be unique too!
			name = Generator.CLASS_TOKEN[classType] + index;
			break;
		case Generator.CS_C_DEPT:
			name = Generator.CLASS_TOKEN[classType] + index;
			break;
		default:
			name = Generator.CLASS_TOKEN[classType] + index;
			break;
		}

		return name;
	}

	/**
	 * Gets the email address of the specified instance.
	 * 
	 * @param classType
	 *            Type of the instance.
	 * @param index
	 *            Index of the instance within its type.
	 * @return The email address of the instance.
	 */
	private String _getEmail(int classType, int index, int university, int dept) {
		String email = "";

		switch (classType) {
		case Generator.CS_C_UNIV:
			email += _getRelativeName(classType, index) + "@"
					+ _getRelativeName(classType, index) + ".edu";
			break;
		case Generator.CS_C_DEPT:
			email += _getRelativeName(classType, index) + "@"
					+ _getRelativeName(classType, index) + "."
					+ _getRelativeName(Generator.CS_C_UNIV, university)
					+ ".edu";
			break;
		default:
			email += _getRelativeName(classType, index) + "@"
					+ _getRelativeName(Generator.CS_C_DEPT, dept) + "."
					+ _getRelativeName(Generator.CS_C_UNIV, university)
					+ ".edu";
			break;
		}

		return email;
	}

}