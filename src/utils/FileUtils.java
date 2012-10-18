package utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FileUtils {

	public static final PathFilter FILTER_ONLY_REALLY_HIDDEN = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith(".");
		}
	};

	public static final PathFilter FILTER_DICTIONARY = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return name.equalsIgnoreCase("_dict");
		}
	};

	public static final PathFilter FILTER_ONLY_HIDDEN = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};

	public static final PathFilter FILTER_ONLY_HIDDEN_AND_TYPE = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".")
					&& !name.endsWith(TriplesUtils.FILE_SUFF_RDF_TYPE);
		}
	};

	public static final PathFilter FILTER_ONLY_SAME_AS = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_SAME_AS));
		}
	};
	

	public static final PathFilter FILTER_ONLY_OWL_SAMEAS = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_SAME_AS));
		}
	};

	public static final PathFilter FILTER_ONLY_SUBPROP_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBPROP));
		}
	};

	public static final PathFilter FILTER_ONLY_FIRST_REST = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX)
							|| name.endsWith(TriplesUtils.FILE_SUFF_RDF_FIRST) || name
								.endsWith(TriplesUtils.FILE_SUFF_RDF_REST));
		}
	};

	public static final PathFilter FILTER_ONLY_SUBCLASS_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS));
		}
	};

	public static final PathFilter FILTER_ONLY_TYPE_SUBCLASS = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX)
							|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS) || name
								.endsWith(TriplesUtils.FILE_SUFF_RDF_TYPE));
		}
	};

	public static final PathFilter FILTER_ONLY_DOMAIN_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_RDFS_DOMAIN));
		}
	};

	public static final PathFilter FILTER_ONLY_RANGE_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_RDFS_RANGE));
		}
	};

	public static final PathFilter FILTER_ONLY_MEMBER_SUBPROP_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_RDFS_MEMBER_SUBPROP));
		}
	};

	public static final PathFilter FILTER_ONLY_RESOURCE_SUBCLAS_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_RDFS_RESOURCE_SUBCLASS));
		}
	};

	public static final PathFilter FILTER_ONLY_LITERAL_SUBCLAS_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_RDFS_LITERAL_SUBCLASS));
		}
	};

	public static final PathFilter FILTER_ONLY_OTHER_SUBCLASS_SUBPROP = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX)
							|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS)
							|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBPROP) || name
								.endsWith(TriplesUtils.FILE_SUFF_OTHER_DATA));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_FUNCTIONAL_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_FUNCTIONAL_PROPERTY_TYPE));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_INVERSE_FUNCTIONAL_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_INV_FUNCTIONAL_PROPERTY_TYPE));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_SYMMETRIC_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_SYMMETRIC_TYPE));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_TRANSITIVE_SCHEMA = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_TRANSITIVE_TYPE));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_INVERSE_OF = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_INVERSE_OF));
		}
	};

	public static final PathFilter FILTER_ONLY_SUBCLASS_SUBPROP_EQ_CLASSPROP = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX)
							|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBCLASS)
							|| name.endsWith(TriplesUtils.FILE_SUFF_RDFS_SUBPROP)
							|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_EQUIVALENT_CLASS) || name
								.endsWith(TriplesUtils.FILE_SUFF_OWL_EQUIVALENT_PROPERTY));
		}
	};

	public static final PathFilter FILTER_ONLY_EQ_CLASSES = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_EQUIVALENT_CLASS));
		}
	};

	public static final PathFilter FILTER_ONLY_EQ_PROPERTIES = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_EQUIVALENT_PROPERTY));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_ON_PROPERTY = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_ON_PROPERTY));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_HAS_VALUE = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_HAS_VALUE));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_SOME_VALUES = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_SOME_VALUES));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_ALL_VALUES = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_ALL_VALUES));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_ON_PROPERTY_HAS_VALUE = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX)
							|| name.endsWith(TriplesUtils.FILE_SUFF_OWL_ON_PROPERTY) || name
								.endsWith(TriplesUtils.FILE_SUFF_OWL_HAS_VALUE));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_CHAIN_PROPERTIES = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_CHAIN_AXIOM_PROPERTY));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_HAS_KEY = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_HAS_KEY));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_INTERSECTION_OF = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_INTERSECTION_OF));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_UNION_OF = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_UNION_OF));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_ONE_OF = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_ONE_OF));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_MAX_CARD = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_MAX_CARD_1));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_MAX_Q_CARD = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_MAX_Q_CARD_1));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_ON_CLASS = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_ON_CLASS));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_TYPE_CLASS = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL_CLASS_TYPE));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_TYPE_DATATYPE_PROP = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_DATATYPE_PROP));
		}
	};

	public static final PathFilter FILTER_ONLY_OWL_TYPE_OBJTYPE_TYPE = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_")
					&& !name.startsWith(".")
					&& (name.startsWith(TriplesUtils.DIR_PREFIX) || name
							.endsWith(TriplesUtils.FILE_SUFF_OWL2_OBJECTTYPE_PROP));
		}
	};
}
