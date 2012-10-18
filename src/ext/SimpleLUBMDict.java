package ext;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class SimpleLUBMDict {

	private Map<String, Long> preloadedURIs = null;

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

	/***** Standard URIs *****/
	public static final String S_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
	public static final String S_RDF_PROPERTY = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>";
	public static final String S_RDFS_RANGE = "<http://www.w3.org/2000/01/rdf-schema#range>";
	public static final String S_RDFS_DOMAIN = "<http://www.w3.org/2000/01/rdf-schema#domain>";
	public static final String S_RDFS_SUBPROPERTY = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
	public static final String S_RDFS_SUBCLASS = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
	public static final String S_RDFS_MEMBER = "<http://www.w3.org/2000/01/rdf-schema#member>";
	public static final String S_RDFS_LITERAL = "<http://www.w3.org/2000/01/rdf-schema#Literal>";
	public static final String S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY = "<http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty>";
	public static final String S_RDFS_DATATYPE = "<http://www.w3.org/2000/01/rdf-schema#Datatype>";
	public static final String S_RDFS_CLASS = "<http://www.w3.org/2000/01/rdf-schema#Class>";
	public static final String S_RDFS_RESOURCE = "<http://www.w3.org/2000/01/rdf-schema#Resource>";
	public static final String S_OWL_CLASS = "<http://www.w3.org/2002/07/owl#Class>";
	public static final String S_OWL_FUNCTIONAL_PROPERTY = "<http://www.w3.org/2002/07/owl#FunctionalProperty>";
	public static final String S_OWL_INVERSE_FUNCTIONAL_PROPERTY = "<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>";
	public static final String S_OWL_SYMMETRIC_PROPERTY = "<http://www.w3.org/2002/07/owl#SymmetricProperty>";
	public static final String S_OWL_TRANSITIVE_PROPERTY = "<http://www.w3.org/2002/07/owl#TransitiveProperty>";
	public static final String S_OWL_SAME_AS = "<http://www.w3.org/2002/07/owl#sameAs>";
	public static final String S_OWL_INVERSE_OF = "<http://www.w3.org/2002/07/owl#inverseOf>";
	public static final String S_OWL_EQUIVALENT_CLASS = "<http://www.w3.org/2002/07/owl#equivalentClass>";
	public static final String S_OWL_EQUIVALENT_PROPERTY = "<http://www.w3.org/2002/07/owl#equivalentProperty>";
	public static final String S_OWL_HAS_VALUE = "<http://www.w3.org/2002/07/owl#hasValue>";
	public static final String S_OWL_ON_PROPERTY = "<http://www.w3.org/2002/07/owl#onProperty>";
	public static final String S_OWL_SOME_VALUES_FROM = "<http://www.w3.org/2002/07/owl#someValuesFrom>";
	public static final String S_OWL_ALL_VALUES_FROM = "<http://www.w3.org/2002/07/owl#allValuesFrom>";

	// /////////////////////////////////////////////////////////////////////////
	// LUBM ontology property information
	// /////////////////////////////////////////////////////////////////////////
	/** property name strings */
	static final String[] propertyNames = { "name", "takesCourse", "teacherOf",
			"undergraduateDegreeFrom", "mastersDegreeFrom",
			"doctoralDegreeFrom", "advisor", "memberOf", "publicationAuthor",
			"headOf", "teachingAssistantOf", "researchInterest",
			"emailAddress", "telephone", "subOrganizationOf", "worksFor" };

	/** class name strings */
	static final String[] classNames = { "University", // CS_C_UNIV
			"Department", // CS_C_DEPT
			"Faculty", // CS_C_FACULTY
			"Professor", // CS_C_PROF
			"FullProfessor", // CS_C_FULLPROF
			"AssociateProfessor", // CS_C_ASSOPROF
			"AssistantProfessor", // CS_C_ASSTPROF
			"Lecturer", // CS_C_LECTURER
			"Student", // CS_C_STUDENT
			"UndergraduateStudent", // CS_C_UNDERSTUD
			"GraduateStudent", // CS_C_GRADSTUD
			"TeachingAssistant", // CS_C_TA
			"ResearchAssistant", // CS_C_RA
			"Course", // CS_C_COURSE
			"GraduateCourse", // CS_C_GRADCOURSE
			"Publication", // CS_C_PUBLICATION
			"Chair", // CS_C_CHAIR
			"Research", // CS_C_RESEARCH
			"ResearchGroup" // CS_C_RESEARCHGROUP
	};

	static final String namespace = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";

	public static void main(String[] args) {
		SimpleLUBMDict dict = new SimpleLUBMDict();
		try {
			dict.encode(args[0]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void encode(String file) throws Exception {
		BufferedReader r = new BufferedReader(new FileReader(file));

		String line = null;
		while ((line = r.readLine()) != null) {
			String[] terms = parseTriple(line);
			System.out.print("{" + getURIId(terms[0]) + ", ");
			System.out.print(getURIId(terms[1]) + ", ");
			System.out.println(getURIId(terms[2]) + "},");
		}
	}

	SimpleLUBMDict() {
		preloadedURIs = new HashMap<String, Long>();

		// Add the standard URIs
		preloadedURIs.put(S_RDF_TYPE.toLowerCase(), RDF_TYPE);
		preloadedURIs.put(S_RDF_PROPERTY.toLowerCase(), RDF_PROPERTY);
		preloadedURIs.put(S_RDFS_RANGE.toLowerCase(), RDFS_RANGE);
		preloadedURIs.put(S_RDFS_DOMAIN.toLowerCase(), RDFS_DOMAIN);
		preloadedURIs.put(S_RDFS_SUBPROPERTY.toLowerCase(), RDFS_SUBPROPERTY);
		preloadedURIs.put(S_RDFS_SUBCLASS.toLowerCase(), RDFS_SUBCLASS);
		preloadedURIs.put(S_RDFS_MEMBER.toLowerCase(), RDFS_MEMBER);
		preloadedURIs.put(S_RDFS_LITERAL.toLowerCase(), RDFS_LITERAL);
		preloadedURIs.put(S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY.toLowerCase(),
				RDFS_CONTAINER_MEMBERSHIP_PROPERTY);
		preloadedURIs.put(S_RDFS_DATATYPE.toLowerCase(), RDFS_DATATYPE);
		preloadedURIs.put(S_RDFS_CLASS.toLowerCase(), RDFS_CLASS);
		preloadedURIs.put(S_RDFS_RESOURCE.toLowerCase(), RDFS_RESOURCE);
		preloadedURIs.put(S_OWL_CLASS.toLowerCase(), OWL_CLASS);
		preloadedURIs.put(S_OWL_FUNCTIONAL_PROPERTY.toLowerCase(),
				OWL_FUNCTIONAL_PROPERTY);
		preloadedURIs.put(S_OWL_INVERSE_FUNCTIONAL_PROPERTY.toLowerCase(),
				OWL_INVERSE_FUNCTIONAL_PROPERTY);
		preloadedURIs.put(S_OWL_SYMMETRIC_PROPERTY.toLowerCase(),
				OWL_SYMMETRIC_PROPERTY);
		preloadedURIs.put(S_OWL_TRANSITIVE_PROPERTY.toLowerCase(),
				OWL_TRANSITIVE_PROPERTY);
		preloadedURIs.put(S_OWL_SAME_AS.toLowerCase(), OWL_SAME_AS);
		preloadedURIs.put(S_OWL_INVERSE_OF.toLowerCase(), OWL_INVERSE_OF);
		preloadedURIs.put(S_OWL_EQUIVALENT_CLASS.toLowerCase(),
				OWL_EQUIVALENT_CLASS);
		preloadedURIs.put(S_OWL_EQUIVALENT_PROPERTY.toLowerCase(),
				OWL_EQUIVALENT_PROPERTY);
		preloadedURIs.put(S_OWL_HAS_VALUE.toLowerCase(), OWL_HAS_VALUE);
		preloadedURIs.put(S_OWL_ON_PROPERTY.toLowerCase(), OWL_ON_PROPERTY);
		preloadedURIs.put(S_OWL_SOME_VALUES_FROM.toLowerCase(),
				OWL_SOME_VALUES_FROM);
		preloadedURIs.put(S_OWL_ALL_VALUES_FROM.toLowerCase(),
				OWL_ALL_VALUES_FROM);

		for (int i = 0; i < propertyNames.length; i++) {
			preloadedURIs.put("<" + namespace + propertyNames[i].toLowerCase()
					+ ">", new Long(i + 64));
		}

		for (int i = 0; i < classNames.length; i++) {
			preloadedURIs.put("<" + namespace + classNames[i].toLowerCase()
					+ ">", new Long(i + 128));
		}
	}

	public Map<String, Long> getMappings() {
		return preloadedURIs;
	}

	private long counter = 256;// This class will give ids over 256

	public Long getURIId(String uri) {
		String u = uri.toLowerCase();
		Long id = preloadedURIs.get(u);
		if (id == null) {
			// System.out.println("Did not find "+uri);
			id = counter << 1;
			counter++;
			if (uri.charAt(0) == '"') {
				id = id + 1;
			}
			preloadedURIs.put(u, id);
		}
		return id;
	}

	public static String[] parseTriple(String triple) throws Exception {
		String[] values = new String[3];

		// Parse subject
		if (triple.startsWith("<")) {
			values[0] = triple.substring(0, triple.indexOf('>') + 1);
		} else { // Is a bnode
			values[0] = triple.substring(0, triple.indexOf(' '));
		}

		triple = triple.substring(values[0].length() + 1);
		// Parse predicate. It can be only a URI
		values[1] = triple.substring(0, triple.indexOf('>') + 1);

		// Parse object
		triple = triple.substring(values[1].length() + 1);
		if (triple.startsWith("<")) { // URI
			values[2] = triple.substring(0, triple.indexOf('>') + 1);
		} else if (triple.charAt(0) == '"') { // Literal
			values[2] = triple.substring(0,
					triple.substring(1).indexOf('"') + 2);
			triple = triple.substring(values[2].length(), triple.length());
			values[2] += triple.substring(0, triple.indexOf(' '));
		} else { // Bnode
			values[2] = triple.substring(0, triple.indexOf(' '));
		}

		/*
		 * char[] cTriple = triple.toCharArray(); values[0] = ""; values[1] =
		 * ""; values[2] = "";
		 * 
		 * 
		 * int currentIdx = 0; int currentPos = 0;
		 * 
		 * while (currentPos < triple.length() && currentIdx < values.length) {
		 * 
		 * if (cTriple[currentPos] == '<') { //URI while (currentPos <
		 * triple.length() && cTriple[currentPos] != '>') { values[currentIdx]
		 * += cTriple[currentPos++]; } currentIdx++; } else if
		 * (cTriple[currentPos] == '"') { //Literal while (currentPos <
		 * triple.length() && cTriple[currentPos] != '"') { values[currentIdx]
		 * += cTriple[currentPos++]; } values[currentIdx] +=
		 * cTriple[currentPos++]; //There could be something more. Read
		 * everything till the next space while (currentPos < triple.length() &&
		 * cTriple[currentPos] != ' ') { values[currentIdx] +=
		 * cTriple[currentPos++]; } currentIdx++; } else if (cTriple[currentPos]
		 * == '_') { //BNode while (currentPos < triple.length() &&
		 * cTriple[currentPos] != ' ') { values[currentIdx] +=
		 * cTriple[currentPos++]; } currentIdx++; }
		 * 
		 * //Eventual spaces currentPos++; }
		 * 
		 * if (currentIdx < 3) throw new
		 * Exception("Not all the members of the triple were parsed");
		 */

		return values;
	}
}