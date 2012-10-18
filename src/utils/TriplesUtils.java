package utils;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.Triple;
import data.TripleSource;

public class TriplesUtils {

	private static TriplesUtils instance = null;

	private Map<String, Long> preloadedURIs = null;
	private static Logger log = LoggerFactory.getLogger(TriplesUtils.class);

	/***** Standard URIs IDs *****/
	public static final long RDF_TYPE = 0;
	public static final long RDF_PROPERTY = 1;
	public static final long RDF_NIL = 28;
	public static final long RDF_LIST = 27;
	public static final long RDF_FIRST = 26;
	public static final long RDF_REST = 25;
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
	public static final long OWL2_PROPERTY_CHAIN_AXIOM = 29;
	public static final long OWL2_HAS_KEY = 30;
	public static final long OWL2_INTERSECTION_OF = 31;
	public static final long OWL2_UNION_OF = 32;
	public static final long OWL2_ONE_OF = 33;
	public static final long OWL2_THING = 34;
	public static final long OWL2_1 = 35;
	public static final long OWL2_MAX_CARD = 36;
	public static final long OWL2_MAX_Q_CARD = 37;
	public static final long OWL2_ON_CLASS = 38;
	public static final long OWL2_NOTHING = 39;
	public static final long OWL2_DATATYPE_PROP = 40;
	public static final long OWL2_OBJECT_PROP = 41;

	/***** Standard URIs *****/
	public static final String S_RDF_NIL = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
	public static final String S_RDF_LIST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#List>";
	public static final String S_RDF_FIRST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
	public static final String S_RDF_REST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
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
	public static final String S_OWL2_PROPERTY_CHAIN_AXIOM = "<http://www.w3.org/2002/07/owl#propertyChainAxiom>";
	public static final String S_OWL2_HAS_KEY = "<http://www.w3.org/2002/07/owl#hasKey>";
	public static final String S_OWL2_INTERSECTION_OF = "<http://www.w3.org/2002/07/owl#intersectionOf>";
	public static final String S_OWL2_UNION_OF = "<http://www.w3.org/2002/07/owl#unionOf>";
	public static final String S_OWL2_ONE_OF = "<http://www.w3.org/2002/07/owl#oneOf>";
	public static final String S_OWL2_THING = "<http://www.w3.org/2002/07/owl#Thing>";
	public static final String S_OWL2_1 = "\"1\"^^http://www.w3.org/2001/XMLSchema#nonNegativeInteger";
	public static final String S_OWL2_MAX_CARD = "<http://www.w3.org/2002/07/owl#maxCardinality>";
	public static final String S_OWL2_MAX_Q_CARD = "<http://www.w3.org/2002/07/owl#maxQualifiedCardinality>";
	public static final String S_OWL2_ON_CLASS = "<http://www.w3.org/2002/07/owl#onClass>";
	public static final String S_OWL2_NOTHING = "<http://www.w3.org/2002/07/owl#Nothing>";
	public static final String S_OWL2_DATATYPE_PROP = "<http://www.w3.org/2002/07/owl#DatatypeProperty>";
	public static final String S_OWL2_OBJECT_PROP = "<http://www.w3.org/2002/07/owl#ObjectProperty>";

	/***** TRIPLES TYPES *****/
	// USED IN RDFS REASONER
	public static final int DATA_TRIPLE = 0;
	public static final int DATA_TRIPLE_TYPE = 5;
	public static final int SCHEMA_TRIPLE_RANGE_PROPERTY = 1;
	public static final int SCHEMA_TRIPLE_DOMAIN_PROPERTY = 2;
	public static final int SCHEMA_TRIPLE_SUBPROPERTY = 3;
	public static final int SCHEMA_TRIPLE_MEMBER_SUBPROPERTY = 20;
	public static final int SCHEMA_TRIPLE_SUBCLASS = 4;
	public static final int SCHEMA_TRIPLE_RESOURCE_SUBCLASS = 21;
	public static final int SCHEMA_TRIPLE_LITERAL_SUBCLASS = 22;

	// USED FOR OWL REASONER
	public static final int SCHEMA_TRIPLE_FUNCTIONAL_PROPERTY = 6;
	public static final int SCHEMA_TRIPLE_INVERSE_FUNCTIONAL_PROPERTY = 7;
	public static final int SCHEMA_TRIPLE_SYMMETRIC_PROPERTY = 8;
	public static final int SCHEMA_TRIPLE_TRANSITIVE_PROPERTY = 9;
	public static final int DATA_TRIPLE_SAME_AS = 10;
	public static final int SCHEMA_TRIPLE_INVERSE_OF = 11;
	public static final int DATA_TRIPLE_CLASS_TYPE = 12;
	public static final int DATA_TRIPLE_PROPERTY_TYPE = 13;
	public static final int SCHEMA_TRIPLE_EQUIVALENT_CLASS = 14;
	public static final int SCHEMA_TRIPLE_EQUIVALENT_PROPERTY = 15;
	public static final int DATA_TRIPLE_HAS_VALUE = 16;
	public static final int SCHEMA_TRIPLE_ON_PROPERTY = 17;
	public static final int SCHEMA_TRIPLE_SOME_VALUES_FROM = 18;
	public static final int SCHEMA_TRIPLE_ALL_VALUES_FROM = 19;

	// USED FOR OWL2 REASONER
	public static final int DATA_TRIPLE_FIRST = 24;
	public static final int DATA_TRIPLE_REST = 25;
	public static final int SCHEMA_TRIPLE_PROPERTY_AXIOM = 26;
	public static final int SCHEMA_TRIPLE_HAS_KEY = 27;
	public static final int SCHEMA_TRIPLE_INTERSECTION_OF = 28;
	public static final int SCHEMA_TRIPLE_UNION_OF = 29;
	public static final int SCHEMA_TRIPLE_ONE_OF = 30;
	public static final int SCHEMA_TRIPLE_SOME_VALUES_FROM_THING = 31;
	public static final int SCHEMA_TRIPLE_MAX_CARD_1 = 32;
	public static final int SCHEMA_TRIPLE_MAX_Q_CARD_1 = 33;
	public static final int SCHEMA_TRIPLE_ON_CLASS = 34;
	public static final int DATA_TRIPLE_DATATYPE = 36;
	public static final int DATA_TRIPLE_OBJECTTYPE = 37;

	// FILE PREFIXES
	public static final String DIR_PREFIX = "dir-";
	public static final String OWL_PREFIX = "owl-";

	// FILE SUFFIXES
	public static final String FILE_SUFF_OTHER_DATA = "-other-data";
	public static final String FILE_SUFF_RDF_TYPE = "-type-data";
	public static final String FILE_SUFF_RDF_FIRST = "-first-data";
	public static final String FILE_SUFF_RDF_REST = "-rest-data";
	public static final String FILE_SUFF_OWL_SYMMETRIC_TYPE = "-symmetric-property-type-data";
	public static final String FILE_SUFF_OWL_TRANSITIVE_TYPE = "-transitive-property-type-data";
	public static final String FILE_SUFF_OWL_CLASS_TYPE = "-owl-class-type-data";
	public static final String FILE_SUFF_OWL_PROPERTY_TYPE = "-owl-property-type-data";
	public static final String FILE_SUFF_OWL_FUNCTIONAL_PROPERTY_TYPE = "-funct-property-type-data";
	public static final String FILE_SUFF_OWL_INV_FUNCTIONAL_PROPERTY_TYPE = "-invfunct-property-type-data";

	public static final String FILE_SUFF_RDFS_SUBCLASS = "-subclas-schema";
	public static final String FILE_SUFF_RDFS_RESOURCE_SUBCLASS = "-resource-subclas-schema";
	public static final String FILE_SUFF_RDFS_LITERAL_SUBCLASS = "-literal-subclas-schema";
	public static final String FILE_SUFF_RDFS_SUBPROP = "-subprop-schema";
	public static final String FILE_SUFF_RDFS_DOMAIN = "-domain-schema";
	public static final String FILE_SUFF_RDFS_RANGE = "-range-schema";
	public static final String FILE_SUFF_RDFS_MEMBER_SUBPROP = "-member-subprop-schema";
	public static final String FILE_SUFF_OWL_SAME_AS = "-same-as-data";
	public static final String FILE_SUFF_OWL_INVERSE_OF = "-inverse-of-schema";
	public static final String FILE_SUFF_OWL_EQUIVALENT_CLASS = "-equivalent-class-schema";
	public static final String FILE_SUFF_OWL_EQUIVALENT_PROPERTY = "-equivalent-property-schema";
	public static final String FILE_SUFF_OWL_HAS_VALUE = "-has-value-data";
	public static final String FILE_SUFF_OWL_ON_PROPERTY = "-on-property-schema";
	public static final String FILE_SUFF_OWL_SOME_VALUES = "-some-values-schema";
	public static final String FILE_SUFF_OWL_ALL_VALUES = "-all-values-schema";
	public static final String FILE_SUFF_OWL2_CHAIN_AXIOM_PROPERTY = "-property-axiom-schema";
	public static final String FILE_SUFF_OWL2_HAS_KEY = "-has-key-schema";
	public static final String FILE_SUFF_OWL2_INTERSECTION_OF = "-intersection-of";
	public static final String FILE_SUFF_OWL2_UNION_OF = "-union-of";
	public static final String FILE_SUFF_OWL2_ONE_OF = "-one-of";
	public static final String FILE_SUFF_OWL2_MAX_CARD_1 = "-max-card-1";
	public static final String FILE_SUFF_OWL2_MAX_Q_CARD_1 = "-max-q-card-1";
	public static final String FILE_SUFF_OWL2_ON_CLASS = "-on-class";
	public static final String FILE_SUFF_OWL_SOME_VALUES_THING = "-thing-some-values-schema";
	public static final String FILE_SUFF_OWL2_DATATYPE_PROP = "-owl-data-type-data";
	public static final String FILE_SUFF_OWL2_OBJECTTYPE_PROP = "-owl-obj-type-data";

	/* XML CONSTANTS */
	public static final String XSD = "http://www.w3.org/2001/XMLSchema#";
	public static final String XSD_INTEGER = XSD + "int";
	public static final String XSD_DECIMAL = XSD + "decimal";
	public static final String XSD_DOUBLE = XSD + "double";

	private TriplesUtils() {
		preloadedURIs = new HashMap<String, Long>();
		preloadedURIs.put(S_RDF_TYPE, RDF_TYPE);
		preloadedURIs.put(S_RDF_PROPERTY, RDF_PROPERTY);
		preloadedURIs.put(S_RDFS_RANGE, RDFS_RANGE);
		preloadedURIs.put(S_RDFS_DOMAIN, RDFS_DOMAIN);
		preloadedURIs.put(S_RDFS_SUBPROPERTY, RDFS_SUBPROPERTY);
		preloadedURIs.put(S_RDFS_SUBCLASS, RDFS_SUBCLASS);
		preloadedURIs.put(S_RDFS_MEMBER, RDFS_MEMBER);
		preloadedURIs.put(S_RDFS_LITERAL, RDFS_LITERAL);
		preloadedURIs.put(S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY,
				RDFS_CONTAINER_MEMBERSHIP_PROPERTY);
		preloadedURIs.put(S_RDFS_DATATYPE, RDFS_DATATYPE);
		preloadedURIs.put(S_RDFS_CLASS, RDFS_CLASS);
		preloadedURIs.put(S_RDFS_RESOURCE, RDFS_RESOURCE);
		preloadedURIs.put(S_OWL_CLASS, OWL_CLASS);
		preloadedURIs.put(S_OWL_FUNCTIONAL_PROPERTY, OWL_FUNCTIONAL_PROPERTY);
		preloadedURIs.put(S_OWL_INVERSE_FUNCTIONAL_PROPERTY,
				OWL_INVERSE_FUNCTIONAL_PROPERTY);
		preloadedURIs.put(S_OWL_SYMMETRIC_PROPERTY, OWL_SYMMETRIC_PROPERTY);
		preloadedURIs.put(S_OWL_TRANSITIVE_PROPERTY, OWL_TRANSITIVE_PROPERTY);
		preloadedURIs.put(S_OWL_SAME_AS, OWL_SAME_AS);
		preloadedURIs.put(S_OWL_INVERSE_OF, OWL_INVERSE_OF);
		preloadedURIs.put(S_OWL_EQUIVALENT_CLASS, OWL_EQUIVALENT_CLASS);
		preloadedURIs.put(S_OWL_EQUIVALENT_PROPERTY, OWL_EQUIVALENT_PROPERTY);
		preloadedURIs.put(S_OWL_HAS_VALUE, OWL_HAS_VALUE);
		preloadedURIs.put(S_OWL_ON_PROPERTY, OWL_ON_PROPERTY);
		preloadedURIs.put(S_OWL_SOME_VALUES_FROM, OWL_SOME_VALUES_FROM);
		preloadedURIs.put(S_OWL_ALL_VALUES_FROM, OWL_ALL_VALUES_FROM);
		preloadedURIs.put(S_RDF_LIST, RDF_LIST);
		preloadedURIs.put(S_RDF_FIRST, RDF_FIRST);
		preloadedURIs.put(S_RDF_REST, RDF_REST);
		preloadedURIs.put(S_RDF_NIL, RDF_NIL);
		preloadedURIs.put(S_OWL2_PROPERTY_CHAIN_AXIOM,
				OWL2_PROPERTY_CHAIN_AXIOM);
		preloadedURIs.put(S_OWL2_HAS_KEY, OWL2_HAS_KEY);
		preloadedURIs.put(S_OWL2_INTERSECTION_OF, OWL2_INTERSECTION_OF);
		preloadedURIs.put(S_OWL2_UNION_OF, OWL2_UNION_OF);
		preloadedURIs.put(S_OWL2_ONE_OF, OWL2_ONE_OF);
		preloadedURIs.put(S_OWL2_THING, OWL2_THING);
		preloadedURIs.put(S_OWL2_1, OWL2_1);
		preloadedURIs.put(S_OWL2_MAX_CARD, OWL2_MAX_CARD);
		preloadedURIs.put(S_OWL2_MAX_Q_CARD, OWL2_MAX_Q_CARD);
		preloadedURIs.put(S_OWL2_ON_CLASS, OWL2_ON_CLASS);
		preloadedURIs.put(S_OWL2_NOTHING, OWL2_NOTHING);
		preloadedURIs.put(S_OWL2_DATATYPE_PROP, OWL2_DATATYPE_PROP);
		preloadedURIs.put(S_OWL2_OBJECT_PROP, OWL2_OBJECT_PROP);

		log.info("cache URIs size: " + preloadedURIs.size());
	}

	public static TriplesUtils getInstance() {
		if (instance == null) {
			instance = new TriplesUtils();
		}
		return instance;
	}

	public Map<String, Long> getPreloadedURIs() {
		return preloadedURIs;
	}

	public static String getFileExtensionByTripleType(TripleSource source,
			Triple triple, String name) {
		int tripleType = getTripleType(source, triple.getSubject(),
				triple.getPredicate(), triple.getObject());

		if (tripleType == SCHEMA_TRIPLE_DOMAIN_PROPERTY) {
			return name + FILE_SUFF_RDFS_DOMAIN;
		}

		if (tripleType == SCHEMA_TRIPLE_RANGE_PROPERTY) {
			return name + FILE_SUFF_RDFS_RANGE;
		}

		if (tripleType == SCHEMA_TRIPLE_SUBPROPERTY) {
			return name + FILE_SUFF_RDFS_SUBPROP;
		}

		if (tripleType == SCHEMA_TRIPLE_MEMBER_SUBPROPERTY) {
			return name + FILE_SUFF_RDFS_MEMBER_SUBPROP;
		}

		if (tripleType == SCHEMA_TRIPLE_SUBCLASS) {
			return name + FILE_SUFF_RDFS_SUBCLASS;
		}

		if (tripleType == SCHEMA_TRIPLE_RESOURCE_SUBCLASS) {
			return name + FILE_SUFF_RDFS_RESOURCE_SUBCLASS;
		}

		if (tripleType == SCHEMA_TRIPLE_LITERAL_SUBCLASS) {
			return name + FILE_SUFF_RDFS_LITERAL_SUBCLASS;
		}

		if (tripleType == DATA_TRIPLE_TYPE) {
			return name + FILE_SUFF_RDF_TYPE;
		}

		// OWL SUBSETS
		if (tripleType == DATA_TRIPLE_CLASS_TYPE) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_CLASS_TYPE;
		}

		if (tripleType == DATA_TRIPLE_PROPERTY_TYPE) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_PROPERTY_TYPE;
		}

		if (tripleType == SCHEMA_TRIPLE_FUNCTIONAL_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL_FUNCTIONAL_PROPERTY_TYPE;
		}

		if (tripleType == SCHEMA_TRIPLE_INVERSE_FUNCTIONAL_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL_INV_FUNCTIONAL_PROPERTY_TYPE;
		}

		if (tripleType == SCHEMA_TRIPLE_SYMMETRIC_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL_SYMMETRIC_TYPE;
		}

		if (tripleType == SCHEMA_TRIPLE_TRANSITIVE_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL_TRANSITIVE_TYPE;
		}

		if (tripleType == DATA_TRIPLE_SAME_AS) {
			return "dir-synonymstable/" + TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL_SAME_AS;
		}

		if (tripleType == SCHEMA_TRIPLE_INVERSE_OF) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_INVERSE_OF;
		}

		if (tripleType == SCHEMA_TRIPLE_EQUIVALENT_CLASS) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL_EQUIVALENT_CLASS;
		}

		if (tripleType == SCHEMA_TRIPLE_EQUIVALENT_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL_EQUIVALENT_PROPERTY;
		}

		if (tripleType == DATA_TRIPLE_HAS_VALUE) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_HAS_VALUE;
		}

		if (tripleType == SCHEMA_TRIPLE_ON_PROPERTY) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_ON_PROPERTY;
		}

		if (tripleType == SCHEMA_TRIPLE_SOME_VALUES_FROM) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_SOME_VALUES;
		}

		if (tripleType == SCHEMA_TRIPLE_ALL_VALUES_FROM) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL_ALL_VALUES;
		}

		/* OWL 2 SUBSET */
		if (tripleType == DATA_TRIPLE_FIRST) {
			return name + FILE_SUFF_RDF_FIRST;
		}

		if (tripleType == DATA_TRIPLE_REST) {
			return name + FILE_SUFF_RDF_REST;
		}

		if (tripleType == SCHEMA_TRIPLE_PROPERTY_AXIOM) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL2_CHAIN_AXIOM_PROPERTY;
		}

		if (tripleType == SCHEMA_TRIPLE_HAS_KEY) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL2_HAS_KEY;
		}

		if (tripleType == SCHEMA_TRIPLE_INTERSECTION_OF) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL2_INTERSECTION_OF;
		}

		if (tripleType == SCHEMA_TRIPLE_UNION_OF) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL2_UNION_OF;
		}

		if (tripleType == SCHEMA_TRIPLE_ONE_OF) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL2_ONE_OF;
		}

		if (tripleType == SCHEMA_TRIPLE_SOME_VALUES_FROM_THING) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL_SOME_VALUES_THING;
		}

		if (tripleType == SCHEMA_TRIPLE_MAX_CARD_1) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL2_MAX_CARD_1;
		}

		if (tripleType == SCHEMA_TRIPLE_MAX_Q_CARD_1) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL2_MAX_Q_CARD_1;
		}

		if (tripleType == SCHEMA_TRIPLE_ON_CLASS) {
			return TriplesUtils.OWL_PREFIX + name + FILE_SUFF_OWL2_ON_CLASS;
		}

		if (tripleType == DATA_TRIPLE_DATATYPE) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL2_DATATYPE_PROP;
		}

		if (tripleType == DATA_TRIPLE_OBJECTTYPE) {
			return TriplesUtils.OWL_PREFIX + name
					+ FILE_SUFF_OWL2_OBJECTTYPE_PROP;
		}

		return name + TriplesUtils.FILE_SUFF_OTHER_DATA;
	}

	public static int getTripleType(TripleSource source, long subject,
			long predicate, long object) {

		int newKey = DATA_TRIPLE;
		if (predicate == RDFS_RANGE) {
			newKey = SCHEMA_TRIPLE_RANGE_PROPERTY;
		} else if (predicate == RDFS_DOMAIN) {
			newKey = SCHEMA_TRIPLE_DOMAIN_PROPERTY;
		} else if (predicate == RDFS_SUBPROPERTY) {
			if (object == RDFS_MEMBER)
				newKey = SCHEMA_TRIPLE_MEMBER_SUBPROPERTY;
			else
				newKey = SCHEMA_TRIPLE_SUBPROPERTY;
		} else if (predicate == RDFS_SUBCLASS) {
			if (object == RDFS_RESOURCE)
				newKey = SCHEMA_TRIPLE_RESOURCE_SUBCLASS;
			else if (object == RDFS_LITERAL)
				newKey = SCHEMA_TRIPLE_LITERAL_SUBCLASS;
			else
				newKey = SCHEMA_TRIPLE_SUBCLASS;
		} else if (predicate == RDF_TYPE) {
			if (object == OWL_CLASS)
				newKey = DATA_TRIPLE_CLASS_TYPE;
			else if (object == RDF_PROPERTY)
				newKey = DATA_TRIPLE_PROPERTY_TYPE;
			else if (object == OWL_FUNCTIONAL_PROPERTY)
				newKey = SCHEMA_TRIPLE_FUNCTIONAL_PROPERTY;
			else if (object == OWL_INVERSE_FUNCTIONAL_PROPERTY)
				newKey = SCHEMA_TRIPLE_INVERSE_FUNCTIONAL_PROPERTY;
			else if (object == OWL_SYMMETRIC_PROPERTY)
				newKey = SCHEMA_TRIPLE_SYMMETRIC_PROPERTY;
			else if (object == OWL_TRANSITIVE_PROPERTY)
				newKey = SCHEMA_TRIPLE_TRANSITIVE_PROPERTY;
			else if (object == OWL2_DATATYPE_PROP)
				newKey = DATA_TRIPLE_DATATYPE;
			else if (object == OWL2_OBJECT_PROP)
				newKey = DATA_TRIPLE_OBJECTTYPE;
			else
				newKey = DATA_TRIPLE_TYPE;
		} else if (predicate == OWL_SAME_AS) {
			newKey = DATA_TRIPLE_SAME_AS;
		} else if (predicate == OWL_INVERSE_OF) {
			newKey = SCHEMA_TRIPLE_INVERSE_OF;
		} else if (predicate == OWL_EQUIVALENT_CLASS) {
			newKey = SCHEMA_TRIPLE_EQUIVALENT_CLASS;
		} else if (predicate == OWL_EQUIVALENT_PROPERTY) {
			newKey = SCHEMA_TRIPLE_EQUIVALENT_PROPERTY;
		} else if (predicate == OWL_HAS_VALUE) {
			newKey = DATA_TRIPLE_HAS_VALUE;
		} else if (predicate == OWL_ON_PROPERTY) {
			newKey = SCHEMA_TRIPLE_ON_PROPERTY;
		} else if (predicate == OWL_SOME_VALUES_FROM) {
			if (object == OWL2_THING)
				newKey = SCHEMA_TRIPLE_SOME_VALUES_FROM_THING;
			else
				newKey = SCHEMA_TRIPLE_SOME_VALUES_FROM;
		} else if (predicate == OWL_ALL_VALUES_FROM) {
			newKey = SCHEMA_TRIPLE_ALL_VALUES_FROM;
		} else if (predicate == RDF_FIRST) {
			newKey = DATA_TRIPLE_FIRST;
		} else if (predicate == RDF_REST) {
			newKey = DATA_TRIPLE_REST;
		} else if (predicate == OWL2_PROPERTY_CHAIN_AXIOM) {
			newKey = SCHEMA_TRIPLE_PROPERTY_AXIOM;
		} else if (predicate == OWL2_HAS_KEY) {
			newKey = SCHEMA_TRIPLE_HAS_KEY;
		} else if (predicate == OWL2_INTERSECTION_OF) {
			newKey = SCHEMA_TRIPLE_INTERSECTION_OF;
		} else if (predicate == OWL2_UNION_OF) {
			newKey = SCHEMA_TRIPLE_UNION_OF;
		} else if (predicate == OWL2_ONE_OF) {
			newKey = SCHEMA_TRIPLE_ONE_OF;
		} else if (predicate == OWL2_MAX_CARD && object == OWL2_1) {
			newKey = SCHEMA_TRIPLE_MAX_CARD_1;
		} else if (predicate == OWL2_MAX_Q_CARD && object == OWL2_1) {
			newKey = SCHEMA_TRIPLE_MAX_Q_CARD_1;
		} else if (predicate == OWL2_ON_CLASS) {
			newKey = SCHEMA_TRIPLE_ON_CLASS;
		}

		return newKey;
	}

	public static String[] parseTriple(String triple, String fileId)
			throws Exception {
		return parseTriple(triple, fileId, true);
	}

	public static String[] parseTriple(String triple, String fileId,
			boolean rewriteBlankNodes) throws Exception {
		String[] values = new String[3];

		// Parse subject
		if (triple.startsWith("<")) {
			values[0] = triple.substring(0, triple.indexOf('>') + 1);
		} else { // Is a bnode
			if (rewriteBlankNodes) {
				values[0] = "_:" + sanitizeBlankNodeName(fileId)
						+ triple.substring(2, triple.indexOf(' ')); // We start
				// from
				// index 2
				// to remove
				// the colon
			} else {
				values[0] = triple.substring(0, triple.indexOf(' '));
			}
		}

		triple = triple.substring(triple.indexOf(' ') + 1);
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
			if (rewriteBlankNodes) {
				values[2] = "_:" + sanitizeBlankNodeName(fileId)
						+ triple.substring(2, triple.indexOf(' ')); // We start
				// from
				// index 2
				// to remove
				// the colon
			} else {
				values[2] = triple.substring(0, triple.indexOf(' '));
			}
		}
		return values;
	}

	/**
	 * Blank node names should start with a letter and have only
	 * letters/numbers. TODO: Is there a case where we have two files that share
	 * all letters/numbers in the same order?
	 * 
	 * @param filename
	 * @return
	 */
	private static String sanitizeBlankNodeName(String filename) {
		StringBuffer ret = new StringBuffer(filename.length());
		if (!filename.isEmpty()) {
			char charAt0 = filename.charAt(0);
			if (Character.isLetter(charAt0))
				ret.append(charAt0);
		}
		for (int i = 1; i < filename.length(); i++) {
			char ch = filename.charAt(i);
			if (Character.isLetterOrDigit(ch)) {
				ret.append(ch);
			}
		}
		return ret.toString();
	}

	public static void createTripleIndex(byte[] bytes, int offset,
			Triple value, String index) {
		if (index.equalsIgnoreCase("spo")) {
			NumberUtils.encodeLong(bytes, offset, value.getSubject());
			NumberUtils.encodeLong(bytes, 8 + offset, value.getPredicate());
			NumberUtils.encodeLong(bytes, 16 + offset, value.getObject());
		} else if (index.equalsIgnoreCase("pos")) {
			NumberUtils.encodeLong(bytes, offset, value.getPredicate());
			NumberUtils.encodeLong(bytes, 8 + offset, value.getObject());
			NumberUtils.encodeLong(bytes, 16 + offset, value.getSubject());
		} else if (index.equalsIgnoreCase("sop")) {
			NumberUtils.encodeLong(bytes, offset, value.getSubject());
			NumberUtils.encodeLong(bytes, 8 + offset, value.getObject());
			NumberUtils.encodeLong(bytes, 16 + offset, value.getPredicate());
		} else if (index.equalsIgnoreCase("osp")) {
			NumberUtils.encodeLong(bytes, offset, value.getObject());
			NumberUtils.encodeLong(bytes, 8 + offset, value.getSubject());
			NumberUtils.encodeLong(bytes, 16 + offset, value.getPredicate());
		} else if (index.equalsIgnoreCase("pso")) {
			NumberUtils.encodeLong(bytes, offset, value.getPredicate());
			NumberUtils.encodeLong(bytes, 8 + offset, value.getSubject());
			NumberUtils.encodeLong(bytes, 16 + offset, value.getObject());
		} else {
			NumberUtils.encodeLong(bytes, offset, value.getObject());
			NumberUtils.encodeLong(bytes, 8 + offset, value.getPredicate());
			NumberUtils.encodeLong(bytes, 16 + offset, value.getSubject());
		}
	}
}
