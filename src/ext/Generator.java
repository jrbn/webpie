/**
 * Heavily modified version of original by Yuanbo Guo
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 * Place, Suite 330, Boston, MA 02111-1307 USA
 */


// We want LUBM 750K


package ext;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Random;

import data.Triple;

public class Generator {

	/**
	 * Gets the LUBM ontology, dictionary encoded
	 * @return
	 */
	 public static ArrayList<Triple> getSchema() {
		  ArrayList<Triple> ret=new ArrayList<Triple>(schema.length);
		  for (long[] a:schema) {
			long s=a[0];
			long p=a[1];
			long o=a[2];
			boolean oIsLiteral= (o>=512) && (o&1)==1;
			ret.add(new Triple(s,p,o,oIsLiteral));
		  }
		  return ret;
	  }
	
	
	
  ///////////////////////////////////////////////////////////////////////////
  //ontology class information
  //NOTE: prefix "CS" was used because the predecessor of univ-bench ontology
  //is called cs ontolgy.
  ///////////////////////////////////////////////////////////////////////////
  /** n/a */
  static final int CS_C_NULL = -1;
  /** University */
  static final int CS_C_UNIV = 0;
  /** Department */
  static final int CS_C_DEPT = CS_C_UNIV + 1;
  /** Faculty */
  static final int CS_C_FACULTY = CS_C_DEPT + 1;
  /** Professor */
  static final int CS_C_PROF = CS_C_FACULTY + 1;
  /** FullProfessor */
  static final int CS_C_FULLPROF = CS_C_PROF + 1;
  /** AssociateProfessor */
  static final int CS_C_ASSOPROF = CS_C_FULLPROF + 1;
  /** AssistantProfessor */
  static final int CS_C_ASSTPROF = CS_C_ASSOPROF + 1;
  /** Lecturer */
  static final int CS_C_LECTURER = CS_C_ASSTPROF + 1;
  /** Student */
  static final int CS_C_STUDENT = CS_C_LECTURER + 1;
  /** UndergraduateStudent */
  static final int CS_C_UNDERSTUD = CS_C_STUDENT + 1;
  /** GraduateStudent */
  static final int CS_C_GRADSTUD = CS_C_UNDERSTUD + 1;
  /** TeachingAssistant */
  static final int CS_C_TA = CS_C_GRADSTUD + 1;
  /** ResearchAssistant */
  static final int CS_C_RA = CS_C_TA + 1;
  /** Course */
  static final int CS_C_COURSE = CS_C_RA + 1;
  /** GraduateCourse */
  static final int CS_C_GRADCOURSE = CS_C_COURSE + 1;
  /** Publication */
  static final int CS_C_PUBLICATION = CS_C_GRADCOURSE + 1;
  /** Chair */
  static final int CS_C_CHAIR = CS_C_PUBLICATION + 1;
  /** Research */
  static final int CS_C_RESEARCH = CS_C_CHAIR + 1;
  /** ResearchGroup */
  static final int CS_C_RESEARCHGROUP = CS_C_RESEARCH + 1;
  /** class information */
  static final int[][] CLASS_INFO = {
      /*{instance number if not specified, direct super class}*/
      //NOTE: the super classes specifed here do not necessarily reflect the entailment of the ontology
      {2, CS_C_NULL}, //CS_C_UNIV
      {1, CS_C_NULL}, //CS_C_DEPT
      {0, CS_C_NULL}, //CS_C_FACULTY
      {0, CS_C_FACULTY}, //CS_C_PROF
      {0, CS_C_PROF}, //CS_C_FULLPROF
      {0, CS_C_PROF}, //CS_C_ASSOPROF
      {0, CS_C_PROF}, //CS_C_ASSTPROF
      {0, CS_C_FACULTY}, //CS_C_LECTURER
      {0, CS_C_NULL}, //CS_C_STUDENT
      {0, CS_C_STUDENT}, //CS_C_UNDERSTUD
      {0, CS_C_STUDENT}, //CS_C_GRADSTUD
      {0, CS_C_NULL}, //CS_C_TA
      {0, CS_C_NULL}, //CS_C_RA
      {0, CS_C_NULL}, //CS_C_COURSE, treated as undergrad course here
      {0, CS_C_NULL}, //CS_C_GRADCOURSE
      {0, CS_C_NULL}, //CS_C_PUBLICATION
      {0, CS_C_NULL}, //CS_C_CHAIR
      {0, CS_C_NULL}, //CS_C_RESEARCH
      {0, CS_C_NULL} //CS_C_RESEARCHGROUP
  };
  
  
  /** class name strings */
  static final String[] CLASS_TOKEN = {
      "University", //CS_C_UNIV
      "Department", //CS_C_DEPT
      "Faculty", //CS_C_FACULTY
      "Professor", //CS_C_PROF
      "FullProfessor", //CS_C_FULLPROF
      "AssociateProfessor", //CS_C_ASSOPROF
      "AssistantProfessor", //CS_C_ASSTPROF
      "Lecturer", //CS_C_LECTURER
      "Student", //CS_C_STUDENT
      "UndergraduateStudent", //CS_C_UNDERSTUD
      "GraduateStudent", //CS_C_GRADSTUD
      "TeachingAssistant", //CS_C_TA
      "ResearchAssistant", //CS_C_RA
      "Course", //CS_C_COURSE
      "GraduateCourse", //CS_C_GRADCOURSE
      "Publication", //CS_C_PUBLICATION
      "Chair", //CS_C_CHAIR
      "Research", //CS_C_RESEARCH
      "ResearchGroup" //CS_C_RESEARCHGROUP
  };
  /** number of classes */
  static final int CLASS_NUM = CLASS_INFO.length;
  /** index of instance-number in the elements of array CLASS_INFO */
  static final int INDEX_NUM = 0;
  /** index of super-class in the elements of array CLASS_INFO */
  static final int INDEX_SUPER = 1;

  ///////////////////////////////////////////////////////////////////////////
  //ontology property information
  ///////////////////////////////////////////////////////////////////////////
  /** name */
  static final int CS_P_NAME = 0;
  /** takesCourse */
  static final int CS_P_TAKECOURSE = CS_P_NAME + 1;
  /** teacherOf */
  static final int CS_P_TEACHEROF = CS_P_TAKECOURSE + 1;
  /** undergraduateDegreeFrom */
  static final int CS_P_UNDERGRADFROM = CS_P_TEACHEROF + 1;
  /** mastersDegreeFrom */
  static final int CS_P_GRADFROM = CS_P_UNDERGRADFROM + 1;
  /** doctoralDegreeFrom */
  static final int CS_P_DOCFROM = CS_P_GRADFROM + 1;
  /** advisor */
  static final int CS_P_ADVISOR = CS_P_DOCFROM + 1;
  /** memberOf */
  static final int CS_P_MEMBEROF = CS_P_ADVISOR + 1;
  /** publicationAuthor */
  static final int CS_P_PUBLICATIONAUTHOR = CS_P_MEMBEROF + 1;
  /** headOf */
  static final int CS_P_HEADOF = CS_P_PUBLICATIONAUTHOR + 1;
  /** teachingAssistantOf */
  static final int CS_P_TAOF = CS_P_HEADOF + 1;
  /** reseachAssistantOf */
  static final int CS_P_RESEARCHINTEREST = CS_P_TAOF + 1;
  /** emailAddress */
  static final int CS_P_EMAIL = CS_P_RESEARCHINTEREST + 1;
  /** telephone */
  static final int CS_P_TELEPHONE = CS_P_EMAIL + 1;
  /** subOrganizationOf */
  static final int CS_P_SUBORGANIZATIONOF = CS_P_TELEPHONE + 1;
  /** worksFor */
  static final int CS_P_WORKSFOR = CS_P_SUBORGANIZATIONOF + 1;
  /** property name strings */
  static final String[] PROP_TOKEN = {
      "name",
      "takesCourse",
      "teacherOf",
      "undergraduateDegreeFrom",
      "mastersDegreeFrom",
      "doctoralDegreeFrom",
      "advisor",
      "memberOf",
      "publicationAuthor",
      "headOf",
      "teachingAssistantOf",
      "researchInterest",
      "emailAddress",
      "telephone",
      "subOrganizationOf",
      "worksFor"
  };
  /** number of properties */
  static final int PROP_NUM = PROP_TOKEN.length;

  ///////////////////////////////////////////////////////////////////////////
  //restrictions for data generation
  ///////////////////////////////////////////////////////////////////////////
  /** size of the pool of the undergraduate courses for one department */
  private static final int UNDER_COURSE_NUM = 100; //must >= max faculty # * FACULTY_COURSE_MAX
  /** size of the pool of the graduate courses for one department */
  private static final int GRAD_COURSE_NUM = 100; //must >= max faculty # * FACULTY_GRADCOURSE_MAX
  /** size of the pool of universities */
  private static final int UNIV_NUM = 1000;
  /** size of the pool of reasearch areas */
  private static final int RESEARCH_NUM = 30;
  /** minimum number of departments in a university */
  private static final int DEPT_MIN = 15;
  /** maximum number of departments in a university */
  private static final int DEPT_MAX = 25;
  //must: DEPT_MAX - DEPT_MIN + 1 <> 2 ^ n
  /** minimum number of publications of a full professor */
  private static final int FULLPROF_PUB_MIN = 15;
  /** maximum number of publications of a full professor */
  private static final int FULLPROF_PUB_MAX = 20;
  /** minimum number of publications of an associate professor */
  private static final int ASSOPROF_PUB_MIN = 10;
  /** maximum number of publications of an associate professor */
  private static final int ASSOPROF_PUB_MAX = 18;
  /** minimum number of publications of an assistant professor */
  private static final int ASSTPROF_PUB_MIN = 5;
  /** maximum number of publications of an assistant professor */
  private static final int ASSTPROF_PUB_MAX = 10;
  /** minimum number of publications of a graduate student */
  private static final int GRADSTUD_PUB_MIN = 0;
  /** maximum number of publications of a graduate student */
  private static final int GRADSTUD_PUB_MAX = 5;
  /** minimum number of publications of a lecturer */
  private static final int LEC_PUB_MIN = 0;
  /** maximum number of publications of a lecturer */
  private static final int LEC_PUB_MAX = 5;
  /** minimum number of courses taught by a faculty */
  private static final int FACULTY_COURSE_MIN = 1;
  /** maximum number of courses taught by a faculty */
  private static final int FACULTY_COURSE_MAX = 2;
  /** minimum number of graduate courses taught by a faculty */
  private static final int FACULTY_GRADCOURSE_MIN = 1;
  /** maximum number of graduate courses taught by a faculty */
  private static final int FACULTY_GRADCOURSE_MAX = 2;
  /** minimum number of courses taken by a undergraduate student */
  private static final int UNDERSTUD_COURSE_MIN = 2;
  /** maximum number of courses taken by a undergraduate student */
  private static final int UNDERSTUD_COURSE_MAX = 4;
  /** minimum number of courses taken by a graduate student */
  private static final int GRADSTUD_COURSE_MIN = 1;
  /** maximum number of courses taken by a graduate student */
  private static final int GRADSTUD_COURSE_MAX = 3;
  /** minimum number of research groups in a department */
  private static final int RESEARCHGROUP_MIN = 10;
  /** maximum number of research groups in a department */
  private static final int RESEARCHGROUP_MAX = 20;
  //faculty number: 30-42
  /** minimum number of full professors in a department*/
  private static final int FULLPROF_MIN = 7;
  /** maximum number of full professors in a department*/
  private static final int FULLPROF_MAX = 10;
  /** minimum number of associate professors in a department*/
  private static final int ASSOPROF_MIN = 10;
  /** maximum number of associate professors in a department*/
  private static final int ASSOPROF_MAX = 14;
  /** minimum number of assistant professors in a department*/
  private static final int ASSTPROF_MIN = 8;
  /** maximum number of assistant professors in a department*/
  private static final int ASSTPROF_MAX = 11;
  /** minimum number of lecturers in a department*/
  private static final int LEC_MIN = 5;
  /** maximum number of lecturers in a department*/
  private static final int LEC_MAX = 7;
  /** minimum ratio of undergraduate students to faculties in a department*/
  private static final int R_UNDERSTUD_FACULTY_MIN = 8;
  /** maximum ratio of undergraduate students to faculties in a department*/
  private static final int R_UNDERSTUD_FACULTY_MAX = 14;
  /** minimum ratio of graduate students to faculties in a department*/
  private static final int R_GRADSTUD_FACULTY_MIN = 3;
  /** maximum ratio of graduate students to faculties in a department*/
  private static final int R_GRADSTUD_FACULTY_MAX = 4;
  //MUST: FACULTY_COURSE_MIN >= R_GRADSTUD_FACULTY_MAX / R_GRADSTUD_TA_MIN;
  /** minimum ratio of graduate students to TA in a department */
  private static final int R_GRADSTUD_TA_MIN = 4;
  /** maximum ratio of graduate students to TA in a department */
  private static final int R_GRADSTUD_TA_MAX = 5;
  /** minimum ratio of graduate students to RA in a department */
  private static final int R_GRADSTUD_RA_MIN = 3;
  /** maximum ratio of graduate students to RA in a department */
  private static final int R_GRADSTUD_RA_MAX = 4;
  /** average ratio of undergraduate students to undergraduate student advising professors */
  private static final int R_UNDERSTUD_ADVISOR = 5;
  /** average ratio of graduate students to graduate student advising professors */
  private static final int R_GRADSTUD_ADVISOR = 1;

  /** delimiter between different parts in an id string*/
  static final char ID_DELIMITER = '/';
  /** delimiter between name and index in a name string of an instance */
  static final char INDEX_DELIMITER = '_';
  /** name of the log file */
  private static final String LOG_FILE = "log.txt";

  /** instance count of a class */
  private class InstanceCount {
    /** instance number within one department */
    public int num = 0;
    /** total instance num including sub-classes within one department */
    public int total = 0;
    /** index of the current instance within the current department */
    public int count = 0;
    /** total number so far within the current department */
    public int logNum = 0;
    /** total number so far */
    public long logTotal = 0l;
  }

  /** instance count of a property */
  private class PropertyCount {
    /** total number so far within the current department */
    public int logNum = 0;
    /** total number so far */
    public long logTotal = 0l;
  }

  /** information a course instance */
  private class CourseInfo {
    /** index of the faculty who teaches this course */
    public int indexInFaculty = 0;
    /** index of this course */
    public int globalIndex = 0;
  }

  /** information of an RA instance */
  private class RaInfo {
    /** index of this RA in the graduate students */
    public int indexInGradStud = 0;
  }

  /** information of a TA instance */
  private class TaInfo {
    /** index of this TA in the graduate students */
    public int indexInGradStud = 0;
    /** index of the course which this TA assists */
    public int indexInCourse = 0; //local index in courses
  }

  /** informaiton of a publication instance */
  private class PublicationInfo {
    /** id */
    public long id;
    /** name */
    public long name;
    /** list of authors */
    public ArrayList authors;
  }

  /** univ-bench ontology url */
  String ontology;
  /** (class) instance information */
  private InstanceCount[] instances_;
  /** property instance information */
  private PropertyCount[] properties_;
  /** random number generator */
  private Random random_;
  /** seed of the random number genertor for the current university */
  private long seed_ = 0l;
  /** user specified seed for the data generation */
  private long baseSeed_ = 0l;
  /** list of undergraduate courses generated so far (in the current department) */
  private ArrayList underCourses_;
  /** list of graduate courses generated so far (in the current department) */
  private ArrayList gradCourses_;
  /** list of remaining available undergraduate courses (in the current department) */
  private ArrayList remainingUnderCourses_;
  /** list of remaining available graduate courses (in the current department) */
  private ArrayList remainingGradCourses_;
  /** list of publication instances generated so far (in the current department) */
  private ArrayList publications_;
  /** index of the full professor who has been chosen as the department chair */
  private int chair_;
  /** starting index of the universities */
  private int startIndex_;
  /** log writer */
  private PrintStream log_ = null;
private DictionaryWriter writer_=new DictionaryWriter();

  /**
   * main method
   */
  public static void main(String[] args) {
    //default values
    int univNum = 1, startIndex = 0, seed = 0;
    boolean daml = false;
    String ontology = null;

    try {
      String arg;
      int i = 0;
      while (i < args.length) {
        arg = args[i++];
        if (arg.equals("-univ")) {
          if (i < args.length) {
            arg = args[i++];
            univNum = Integer.parseInt(arg);
            if (univNum < 1)
              throw new NumberFormatException();
          }
          else
            throw new NumberFormatException();
        }
        else if (arg.equals("-index")) {
          if (i < args.length) {
            arg = args[i++];
            startIndex = Integer.parseInt(arg);
            if (startIndex < 0)
              throw new NumberFormatException();
          }
          else
            throw new NumberFormatException();
        }
        else if (arg.equals("-seed")) {
          if (i < args.length) {
            arg = args[i++];
            seed = Integer.parseInt(arg);
            if (seed < 0)
              throw new NumberFormatException();
          }
          else
            throw new NumberFormatException();
        }
        else if (arg.equals("-daml")) {
          daml = true;
        }
        else if (arg.equals("-onto")) {
          if (i < args.length) {
            arg = args[i++];
            ontology = arg;
          }
          else
            throw new Exception();
        }
        else
          throw new Exception();
      }
      if ( ( (long) startIndex + univNum - 1) > Integer.MAX_VALUE) {
        System.err.println("Index overflow!");
        throw new Exception();
      }
      if (null == ontology) {
        System.err.println("ontology url is requested!");
        throw new Exception();
      }
    }
    catch (Exception e) {
      System.err.println("Usage: Generator\n" +
                         "\t[-univ <num of universities(1~" + Integer.MAX_VALUE +
                         ")>]\n" +
                         "\t[-index <start index(0~" + Integer.MAX_VALUE +
                         ")>]\n" +
                         "\t[-seed <seed(0~" + Integer.MAX_VALUE + ")>]\n" +
                         "\t[-daml]\n" +
                         "\t-onto <univ-bench ontology url>");
      System.exit(0);
    }

    new Generator(univNum, startIndex, seed);
  }

 /**
  * Makes a new university dataset
  * @param univNum number of universities
  * @param startIndex the index of the first university
  * @param seed random seed
  */
  public Generator(int univNum, int startIndex, int seed) {
    instances_ = new InstanceCount[CLASS_NUM];
    for (int i = 0; i < CLASS_NUM; i++) {
      instances_[i] = new InstanceCount();
    }
    properties_ = new PropertyCount[PROP_NUM];
    for (int i = 0; i < PROP_NUM; i++) {
      properties_[i] = new PropertyCount();
    }

    random_ = new Random();
    underCourses_ = new ArrayList();
    gradCourses_ = new ArrayList();
    remainingUnderCourses_ = new ArrayList();
    remainingGradCourses_ = new ArrayList();
    publications_ = new ArrayList();
    
    startIndex_ = startIndex;
    baseSeed_ = seed;
    instances_[CS_C_UNIV].num = univNum;
    instances_[CS_C_UNIV].count = startIndex;
  }

  /**
   * Gets the instance data, dictionary encoded
   * @return
   */
  public ArrayList<Triple> getData() {
	 _generate();
	 return writer_.getData();
  }
  

  ///////////////////////////////////////////////////////////////////////////
  //writer callbacks

  /**
   * Callback by the writer when it starts an instance section.
   * @param classType Type of the instance.
   */
  void startSectionCB(int classType) {
    instances_[classType].logNum++;
    instances_[classType].logTotal++;
  }

  /**
   * Callback by the writer when it starts an instance section identified by an rdf:about attribute.
   * @param classType Type of the instance.
   */
  void startAboutSectionCB(int classType) {
    startSectionCB(classType);
  }

  /**
   * Callback by the writer when it adds a property statement.
   * @param property Type of the property.
   */
  void addPropertyCB(int property) {
    properties_[property].logNum++;
    properties_[property].logTotal++;
  }

  /**
   * Callback by the writer when it adds a property statement whose value is an individual.
   * @param classType Type of the individual.
   */
  void addValueClassCB(int classType) {
    instances_[classType].logNum++;
    instances_[classType].logTotal++;
  }

  ///////////////////////////////////////////////////////////////////////////

  /**
   * Sets instance specification.
   */
  private void _setInstanceInfo() {
    int subClass, superClass;

    for (int i = 0; i < CLASS_NUM; i++) {
      switch (i) {
        case CS_C_UNIV:
          break;
        case CS_C_DEPT:
          break;
        case CS_C_FULLPROF:
          instances_[i].num = _getRandomFromRange(FULLPROF_MIN, FULLPROF_MAX);
          break;
        case CS_C_ASSOPROF:
          instances_[i].num = _getRandomFromRange(ASSOPROF_MIN, ASSOPROF_MAX);
          break;
        case CS_C_ASSTPROF:
          instances_[i].num = _getRandomFromRange(ASSTPROF_MIN, ASSTPROF_MAX);
          break;
        case CS_C_LECTURER:
          instances_[i].num = _getRandomFromRange(LEC_MIN, LEC_MAX);
          break;
        case CS_C_UNDERSTUD:
          instances_[i].num = _getRandomFromRange(R_UNDERSTUD_FACULTY_MIN *
                                         instances_[CS_C_FACULTY].total,
                                         R_UNDERSTUD_FACULTY_MAX *
                                         instances_[CS_C_FACULTY].total);
          break;
        case CS_C_GRADSTUD:
          instances_[i].num = _getRandomFromRange(R_GRADSTUD_FACULTY_MIN *
                                         instances_[CS_C_FACULTY].total,
                                         R_GRADSTUD_FACULTY_MAX *
                                         instances_[CS_C_FACULTY].total);
          break;
        case CS_C_TA:
          instances_[i].num = _getRandomFromRange(instances_[CS_C_GRADSTUD].total /
                                         R_GRADSTUD_TA_MAX,
                                         instances_[CS_C_GRADSTUD].total /
                                         R_GRADSTUD_TA_MIN);
          break;
        case CS_C_RA:
          instances_[i].num = _getRandomFromRange(instances_[CS_C_GRADSTUD].total /
                                         R_GRADSTUD_RA_MAX,
                                         instances_[CS_C_GRADSTUD].total /
                                         R_GRADSTUD_RA_MIN);
          break;
        case CS_C_RESEARCHGROUP:
          instances_[i].num = _getRandomFromRange(RESEARCHGROUP_MIN, RESEARCHGROUP_MAX);
          break;
        default:
          instances_[i].num = CLASS_INFO[i][INDEX_NUM];
          break;
      }
      instances_[i].total = instances_[i].num;
      subClass = i;
      while ( (superClass = CLASS_INFO[subClass][INDEX_SUPER]) != CS_C_NULL) {
        instances_[superClass].total += instances_[i].num;
        subClass = superClass;
      }
    }
  }

  /** Begins data generation according to the specification */
  private void _generate() {
      for (int i = 0; i < instances_[CS_C_UNIV].num; i++) 
        _generateUniv(i + startIndex_);
  }

  /**
   * Creates a university.
   * @param index Index of the university.
   */
  private void _generateUniv(int index) {
    //this transformation guarantees no different pairs of (index, baseSeed) generate the same data
    seed_ = baseSeed_ * (Integer.MAX_VALUE + 1) + index;
    random_.setSeed(seed_);

    //determine department number
    instances_[CS_C_DEPT].num = _getRandomFromRange(DEPT_MIN, DEPT_MAX);
    instances_[CS_C_DEPT].count = 0;
    //generate departments
    for (int i = 0; i < instances_[CS_C_DEPT].num; i++) {
      _generateDept(index, i);
    }
  }

  /**
   * Creates a department.
   * @param univIndex Index of the current university.
   * @param index Index of the department.
   * NOTE: Use univIndex instead of instances[CS_C_UNIV].count till generateASection(CS_C_UNIV, ) is invoked.
   */
  private void _generateDept(int univIndex, int index) {

    //reset
    _setInstanceInfo();
    underCourses_.clear();
    gradCourses_.clear();
    remainingUnderCourses_.clear();
    remainingGradCourses_.clear();
    for (int i = 0; i < UNDER_COURSE_NUM; i++) {
      remainingUnderCourses_.add(new Integer(i));
    }
    for (int i = 0; i < GRAD_COURSE_NUM; i++) {
      remainingGradCourses_.add(new Integer(i));
    }
    publications_.clear();
    for (int i = 0; i < CLASS_NUM; i++) {
      instances_[i].logNum = 0;
    }
    for (int i = 0; i < PROP_NUM; i++) {
      properties_[i].logNum = 0;
    }

    //decide the chair
    chair_ = random_.nextInt(instances_[CS_C_FULLPROF].total);

    if (index == 0) {
      _generateASection(CS_C_UNIV, univIndex);
    }
    _generateASection(CS_C_DEPT, index);
    for (int i = CS_C_DEPT + 1; i < CLASS_NUM; i++) {
      instances_[i].count = 0;
      for (int j = 0; j < instances_[i].num; j++) {
        _generateASection(i, j);
      }
    }

    _generatePublications();
    _generateCourses();
    _generateRaTa();
  }

  ///////////////////////////////////////////////////////////////////////////
  //instance generation

  /**
   * Generates an instance of the specified class
   * @param classType Type of the instance.
   * @param index Index of the instance.
   */
  private void _generateASection(int classType, int index) {
    _updateCount(classType);

    switch (classType) {
      case CS_C_UNIV:
        _generateAUniv(index);
        break;
      case CS_C_DEPT:
        _generateADept(index);
        break;
      case CS_C_FACULTY:
        _generateAFaculty(index);
        break;
      case CS_C_PROF:
        _generateAProf(index);
        break;
      case CS_C_FULLPROF:
        _generateAFullProf(index);
        break;
      case CS_C_ASSOPROF:
        _generateAnAssociateProfessor(index);
        break;
      case CS_C_ASSTPROF:
        _generateAnAssistantProfessor(index);
        break;
      case CS_C_LECTURER:
        _generateALecturer(index);
        break;
      case CS_C_UNDERSTUD:
        _generateAnUndergraduateStudent(index);
        break;
      case CS_C_GRADSTUD:
        _generateAGradudateStudent(index);
        break;
      case CS_C_COURSE:
        _generateACourse(index);
        break;
      case CS_C_GRADCOURSE:
        _generateAGraduateCourse(index);
        break;
      case CS_C_RESEARCHGROUP:
        _generateAResearchGroup(index);
        break;
      default:
        break;
    }
  }

  /**
   * Generates a university instance.
   * @param index Index of the instance.
   */
  private void _generateAUniv(int index) {
    writer_.startSection(CS_C_UNIV, getId(CS_C_UNIV, index));
    writer_.addProperty(CS_P_NAME, getRelativeName(CS_C_UNIV, index), false);
    writer_.endSection(CS_C_UNIV);
  }

  /**
   * Generates a department instance.
   * @param index Index of the department.
   */
  private void _generateADept(int index) {
    writer_.startSection(CS_C_DEPT, getId(CS_C_DEPT, index));
    writer_.addProperty(CS_P_NAME, getRelativeName(CS_C_DEPT, index), false);
    writer_.addProperty(CS_P_SUBORGANIZATIONOF, CS_C_UNIV,
                       getId(CS_C_UNIV, instances_[CS_C_UNIV].count - 1));
    writer_.endSection(CS_C_DEPT);
  }

  /**
   * Generates a faculty instance.
   * @param index Index of the faculty.
   */
  private void _generateAFaculty(int index) {
    writer_.startSection(CS_C_FACULTY, getId(CS_C_FACULTY, index));
    _generateAFaculty_a(CS_C_FACULTY, index);
    writer_.endSection(CS_C_FACULTY);
  }

  /**
   * Generates properties for the specified faculty instance.
   * @param type Type of the faculty.
   * @param index Index of the instance within its type.
   */
  private void _generateAFaculty_a(int type, int index) {
    int indexInFaculty;
    int courseNum;
    int courseIndex;
    boolean dup;
    CourseInfo course;

    indexInFaculty = instances_[CS_C_FACULTY].count - 1;

    writer_.addProperty(CS_P_NAME, getRelativeName(type, index), false);

    //undergradutate courses
    courseNum = _getRandomFromRange(FACULTY_COURSE_MIN, FACULTY_COURSE_MAX);
    for (int i = 0; i < courseNum; i++) {
      courseIndex = _AssignCourse(indexInFaculty);
      writer_.addProperty(CS_P_TEACHEROF, getId(CS_C_COURSE, courseIndex), true);
    }
    //gradutate courses
    courseNum = _getRandomFromRange(FACULTY_GRADCOURSE_MIN, FACULTY_GRADCOURSE_MAX);
    for (int i = 0; i < courseNum; i++) {
      courseIndex = _AssignGraduateCourse(indexInFaculty);
      writer_.addProperty(CS_P_TEACHEROF, getId(CS_C_GRADCOURSE, courseIndex), true);
    }
    //person properties
    writer_.addProperty(CS_P_UNDERGRADFROM, CS_C_UNIV,
                       getId(CS_C_UNIV, random_.nextInt(UNIV_NUM)));
    writer_.addProperty(CS_P_GRADFROM, CS_C_UNIV,
                       getId(CS_C_UNIV, random_.nextInt(UNIV_NUM)));
    writer_.addProperty(CS_P_DOCFROM, CS_C_UNIV,
                       getId(CS_C_UNIV, random_.nextInt(UNIV_NUM)));
    writer_.addProperty(CS_P_WORKSFOR,
                       getId(CS_C_DEPT, instances_[CS_C_DEPT].count - 1), true);
    writer_.addProperty(CS_P_EMAIL, getLiteral(type, index), false);
    writer_.addProperty(CS_P_TELEPHONE, Long.MAX_VALUE, false);
  }

  /**
   * Assigns an undergraduate course to the specified faculty.
   * @param indexInFaculty Index of the faculty.
   * @return Index of the selected course in the pool.
   */
  private int _AssignCourse(int indexInFaculty) {
    //NOTE: this line, although overriden by the next one, is deliberately kept
    // to guarantee identical random number generation to the previous version.
    int pos = _getRandomFromRange(0, remainingUnderCourses_.size() - 1);
    pos = 0; //fetch courses in sequence

    CourseInfo course = new CourseInfo();
    course.indexInFaculty = indexInFaculty;
    course.globalIndex = ( (Integer) remainingUnderCourses_.get(pos)).intValue();
    underCourses_.add(course);

    remainingUnderCourses_.remove(pos);

    return course.globalIndex;
  }

  /**
   * Assigns a graduate course to the specified faculty.
   * @param indexInFaculty Index of the faculty.
   * @return Index of the selected course in the pool.
   */
  private int _AssignGraduateCourse(int indexInFaculty) {
    //NOTE: this line, although overriden by the next one, is deliberately kept
    // to guarantee identical random number generation to the previous version.
    int pos = _getRandomFromRange(0, remainingGradCourses_.size() - 1);
    pos = 0; //fetch courses in sequence

    CourseInfo course = new CourseInfo();
    course.indexInFaculty = indexInFaculty;
    course.globalIndex = ( (Integer) remainingGradCourses_.get(pos)).intValue();
    gradCourses_.add(course);

    remainingGradCourses_.remove(pos);

    return course.globalIndex;
  }

  /**
   * Generates a professor instance.
   * @param index Index of the professor.
   */
  private void _generateAProf(int index) {
    writer_.startSection(CS_C_PROF, getId(CS_C_PROF, index));
    _generateAProf_a(CS_C_PROF, index);
    writer_.endSection(CS_C_PROF);
  }

  /**
   * Generates properties for a professor instance.
   * @param type Type of the professor.
   * @param index Index of the intance within its type.
   */
  private void _generateAProf_a(int type, int index) {
    _generateAFaculty_a(type, index);
    writer_.addProperty(CS_P_RESEARCHINTEREST,
                       getRelativeName(CS_C_RESEARCH,
                                       random_.nextInt(RESEARCH_NUM)), false);
  }

  /**
   * Generates a full professor instances.
   * @param index Index of the full professor.
   */
  private void _generateAFullProf(int index) {
    long id;

    id = getId(CS_C_FULLPROF, index);
    writer_.startSection(CS_C_FULLPROF, id);
    _generateAProf_a(CS_C_FULLPROF, index);
    if (index == chair_) {
      writer_.addProperty(CS_P_HEADOF,
                         getId(CS_C_DEPT, instances_[CS_C_DEPT].count - 1), true);
    }
    writer_.endSection(CS_C_FULLPROF);
    _assignFacultyPublications(id, FULLPROF_PUB_MIN, FULLPROF_PUB_MAX);
  }

  /**
   * Generates an associate professor instance.
   * @param index Index of the associate professor.
   */
  private void _generateAnAssociateProfessor(int index) {
    long id = getId(CS_C_ASSOPROF, index);
    writer_.startSection(CS_C_ASSOPROF, id);
    _generateAProf_a(CS_C_ASSOPROF, index);
    writer_.endSection(CS_C_ASSOPROF);
    _assignFacultyPublications(id, ASSOPROF_PUB_MIN, ASSOPROF_PUB_MAX);
  }

  /**
   * Generates an assistant professor instance.
   * @param index Index of the assistant professor.
   */
  private void _generateAnAssistantProfessor(int index) {
    long id = getId(CS_C_ASSTPROF, index);
    writer_.startSection(CS_C_ASSTPROF, id);
    _generateAProf_a(CS_C_ASSTPROF, index);
    writer_.endSection(CS_C_ASSTPROF);
    _assignFacultyPublications(id, ASSTPROF_PUB_MIN, ASSTPROF_PUB_MAX);
  }

  /**
   * Generates a lecturer instance.
   * @param index Index of the lecturer.
   */
  private void _generateALecturer(int index) {
    long id = getId(CS_C_LECTURER, index);
    writer_.startSection(CS_C_LECTURER, id);
    _generateAFaculty_a(CS_C_LECTURER, index);
    writer_.endSection(CS_C_LECTURER);
    _assignFacultyPublications(id, LEC_PUB_MIN, LEC_PUB_MAX);
  }

  /**
   * Assigns publications to the specified faculty.
   * @param author Id of the faculty
   * @param min Minimum number of publications
   * @param max Maximum number of publications
   */
  private void _assignFacultyPublications(long author, int min, int max) {
    int num;
    PublicationInfo publication;

    num = _getRandomFromRange(min, max);
    for (int i = 0; i < num; i++) {
      publication = new PublicationInfo();
      publication.id = getPublicationId(i, author);
      publication.name = getRelativeName(CS_C_PUBLICATION, i) | 1;
      publication.authors = new ArrayList();
      publication.authors.add(author);
      publications_.add(publication);
    }
  }

  /**
   * Assigns publications to the specified graduate student. The publications are
   * chosen from some faculties'.
   * @param author Id of the graduate student.
   * @param min Minimum number of publications.
   * @param max Maximum number of publications.
   */
  private void _assignGraduateStudentPublications(long author, int min, int max) {
    int num;
    PublicationInfo publication;

    num = _getRandomFromRange(min, max);
    ArrayList list = _getRandomList(num, 0, publications_.size() - 1);
    for (int i = 0; i < list.size(); i++) {
      publication = (PublicationInfo) publications_.get( ( (Integer) list.get(i)).
                                               intValue());
      publication.authors.add(author);
    }
  }

  /**
   * Generates publication instances. These publications are assigned to some faculties
   * and graduate students before.
   */
  private void _generatePublications() {
    for (int i = 0; i < publications_.size(); i++) {
      _generateAPublication( (PublicationInfo) publications_.get(i));
    }
  }

  /**
   * Generates a publication instance.
   * @param publication Information of the publication.
   */
  private void _generateAPublication(PublicationInfo publication) {
    writer_.startSection(CS_C_PUBLICATION, publication.id);
    writer_.addProperty(CS_P_NAME, publication.name, false);
    for (int i = 0; i < publication.authors.size(); i++) {
      writer_.addProperty(CS_P_PUBLICATIONAUTHOR,
                         (Long) publication.authors.get(i), true);
    }
    writer_.endSection(CS_C_PUBLICATION);
  }

  /**
   * Generates properties for the specified student instance.
   * @param type Type of the student.
   * @param index Index of the instance within its type.
   */
  private void _generateAStudent_a(int type, int index) {
    writer_.addProperty(CS_P_NAME, getRelativeName(type, index), false);
    writer_.addProperty(CS_P_MEMBEROF,
                       getId(CS_C_DEPT, instances_[CS_C_DEPT].count - 1), true);
    writer_.addProperty(CS_P_EMAIL, getLiteral(type, index), false);
    writer_.addProperty(CS_P_TELEPHONE, Long.MAX_VALUE, false);
  }

  /**
   * Generates an undergraduate student instance.
   * @param index Index of the undergraduate student.
   */
  private void _generateAnUndergraduateStudent(int index) {
    int n;
    ArrayList list;

    writer_.startSection(CS_C_UNDERSTUD, getId(CS_C_UNDERSTUD, index));
    _generateAStudent_a(CS_C_UNDERSTUD, index);
    n = _getRandomFromRange(UNDERSTUD_COURSE_MIN, UNDERSTUD_COURSE_MAX);
    list = _getRandomList(n, 0, underCourses_.size() - 1);
    for (int i = 0; i < list.size(); i++) {
      CourseInfo info = (CourseInfo) underCourses_.get( ( (Integer) list.get(i)).
          intValue());
      writer_.addProperty(CS_P_TAKECOURSE, getId(CS_C_COURSE, info.globalIndex), true);
    }
    if (0 == random_.nextInt(R_UNDERSTUD_ADVISOR)) {
      writer_.addProperty(CS_P_ADVISOR, _selectAdvisor(), true);
    }
    writer_.endSection(CS_C_UNDERSTUD);
  }

  /**
   * Generates a graduate student instance.
   * @param index Index of the graduate student.
   */
  private void _generateAGradudateStudent(int index) {
    int n;
    ArrayList list;
    long id;

    id = getId(CS_C_GRADSTUD, index);
    writer_.startSection(CS_C_GRADSTUD, id);
    _generateAStudent_a(CS_C_GRADSTUD, index);
    n = _getRandomFromRange(GRADSTUD_COURSE_MIN, GRADSTUD_COURSE_MAX);
    list = _getRandomList(n, 0, gradCourses_.size() - 1);
    for (int i = 0; i < list.size(); i++) {
      CourseInfo info = (CourseInfo) gradCourses_.get( ( (Integer) list.get(i)).
          intValue());
      writer_.addProperty(CS_P_TAKECOURSE,
                         getId(CS_C_GRADCOURSE, info.globalIndex), true);
    }
    writer_.addProperty(CS_P_UNDERGRADFROM, CS_C_UNIV,
                       getId(CS_C_UNIV, random_.nextInt(UNIV_NUM)));
    if (0 == random_.nextInt(R_GRADSTUD_ADVISOR)) {
      writer_.addProperty(CS_P_ADVISOR, _selectAdvisor(), true);
    }
    _assignGraduateStudentPublications(id, GRADSTUD_PUB_MIN, GRADSTUD_PUB_MAX);
    writer_.endSection(CS_C_GRADSTUD);
  }

  /**
   * Select an advisor from the professors.
   * @return Id of the selected professor.
   */
  private long _selectAdvisor() {
    int profType;
    int index;

    profType = _getRandomFromRange(CS_C_FULLPROF, CS_C_ASSTPROF);
    index = random_.nextInt(instances_[profType].total);
    return getId(profType, index);
  }

  /**
   * Generates a TA instance according to the specified information.
   * @param ta Information of the TA.
   */
  private void _generateATa(TaInfo ta) {
    writer_.startAboutSection(CS_C_TA, getId(CS_C_GRADSTUD, ta.indexInGradStud));
    writer_.addProperty(CS_P_TAOF, getId(CS_C_COURSE, ta.indexInCourse), true);
    writer_.endSection(CS_C_TA);
  }

  /**
   * Generates an RA instance according to the specified information.
   * @param ra Information of the RA.
   */
  private void _generateAnRa(RaInfo ra) {
    writer_.startAboutSection(CS_C_RA, getId(CS_C_GRADSTUD, ra.indexInGradStud));
    writer_.endSection(CS_C_RA);
  }

  /**
   * Generates a course instance.
   * @param index Index of the course.
   */
  private void _generateACourse(int index) {
    writer_.startSection(CS_C_COURSE, getId(CS_C_COURSE, index));
    writer_.addProperty(CS_P_NAME,
                       getRelativeName(CS_C_COURSE, index), false);
    writer_.endSection(CS_C_COURSE);
  }

  /**
   * Generates a graduate course instance.
   * @param index Index of the graduate course.
   */
  private void _generateAGraduateCourse(int index) {
    writer_.startSection(CS_C_GRADCOURSE, getId(CS_C_GRADCOURSE, index));
    writer_.addProperty(CS_P_NAME,
                       getRelativeName(CS_C_GRADCOURSE, index), false);
    writer_.endSection(CS_C_GRADCOURSE);
  }

  /**
   * Generates course/graduate course instances. These course are assigned to some
   * faculties before.
   */
  private void _generateCourses() {
    for (int i = 0; i < underCourses_.size(); i++) {
      _generateACourse( ( (CourseInfo) underCourses_.get(i)).globalIndex);
    }
    for (int i = 0; i < gradCourses_.size(); i++) {
      _generateAGraduateCourse( ( (CourseInfo) gradCourses_.get(i)).globalIndex);
    }
  }

  /**
   * Chooses RAs and TAs from graduate student and generates their instances accordingly.
   */
  private void _generateRaTa() {
    ArrayList list, courseList;
    TaInfo ta;
    RaInfo ra;
    ArrayList tas, ras;
    int i;

    tas = new ArrayList();
    ras = new ArrayList();
    list = _getRandomList(instances_[CS_C_TA].total + instances_[CS_C_RA].total,
                      0, instances_[CS_C_GRADSTUD].total - 1);
    courseList = _getRandomList(instances_[CS_C_TA].total, 0,
                            underCourses_.size() - 1);

    for (i = 0; i < instances_[CS_C_TA].total; i++) {
      ta = new TaInfo();
      ta.indexInGradStud = ( (Integer) list.get(i)).intValue();
      ta.indexInCourse = ( (CourseInfo) underCourses_.get( ( (Integer)
          courseList.get(i)).intValue())).globalIndex;
      _generateATa(ta);
    }
    while (i < list.size()) {
      ra = new RaInfo();
      ra.indexInGradStud = ( (Integer) list.get(i)).intValue();
      _generateAnRa(ra);
      i++;
    }
  }

  /**
   * Generates a research group instance.
   * @param index Index of the research group.
   */
  private void _generateAResearchGroup(int index) {
    long id;
    id = getId(CS_C_RESEARCHGROUP, index);
    writer_.startSection(CS_C_RESEARCHGROUP, id);
    writer_.addProperty(CS_P_SUBORGANIZATIONOF,
                       getId(CS_C_DEPT, instances_[CS_C_DEPT].count - 1), true);
    writer_.endSection(CS_C_RESEARCHGROUP);
  }

  ///////////////////////////////////////////////////////////////////////////


  
  /* OLD
   * 62: Flag for types with relative name
   * 61: Flag for publications
   * 60: Flag to indicate not well-known ids // Rooms for lots
   * 30-59: University // Room for 1G universities
   * 23-29: Department // Room for 64 departments 
   * 16-22: Classtype // Room for 64 types
   * 8-15: Index of thing // Room for 128
   * 1-7: Publication // Used only for publications
   * 0: flag for literals
   * 
   */
  
  /* NEW
   * 62: Flag for types with relative name
   * 61: Flag for publications
   * 60: Flag to indicate not well-known ids // Rooms for lots
   * 38-59: University // Room for 2M universities
   * 31-37: Department // Room for 64 departments 
   * 24-30: Classtype // Room for 64 types
   * 8-23: Index of thing // Room for 32768
   * 1-7: Publication // Used only for publications
   * 0: flag for literals
   * 
   */
  
  
  /**
   * Gets the id of the specified instance.
   * @param classType Type of the instance.
   * @param index Index of the instance within its type.
   * @return Id of the instance.
   */
  private long getId(int classType, int index) {
    long id = ((long)1<<60) | (classType<<24) ;
    long uni=instances_[CS_C_UNIV].count - 1;
    long dept=instances_[CS_C_DEPT].count - 1;

    switch (classType) {
      case CS_C_UNIV:
    	id |= ((long)index<<38);
        break;
      case CS_C_DEPT:
        id |=  (uni<<38)| (index<<8);
        break;
      default:
        id |= (dept<<31) | (uni<<38)| (index<<8);
        break;
    }
    return id;
  }
  
  
  private long getPublicationId(int index, long authorID) {
    return authorID | ((index+1)<<1);
  }
  
  
  

  /**
   * Gets the name of the specified instance that is unique within a department.
   * @param classType Type of the instance.
   * @param index Index of the instance within its type.
   * @return Name of the instance.
   */
  private long getRelativeName(int classType, int index) {
		long id=((long)1)<<60; 
		id|=((long)1)<<62;
		id|=classType<<24;
		id|=index<<8;
		return id;
  }
  
  
  /**
   * Gets the email address of the specified instance.
   * @param classType Type of the instance.
   * @param index Index of the instance within its type.
   * @return The email address of the instance.
   */
  private long getLiteral(int classType, int index) {
    long email = 1; // 1 because it is a literal

    email |= getId(classType, index);

    return email;
  }
  


  /**
   * Increases by 1 the instance count of the specified class. This also includes
   * the increase of the instacne count of all its super class.
   * @param classType Type of the instance.
   */
  private void _updateCount(int classType) {
    int subClass, superClass;

    instances_[classType].count++;
    subClass = classType;
    while ( (superClass = CLASS_INFO[subClass][INDEX_SUPER]) != CS_C_NULL) {
      instances_[superClass].count++;
      subClass = superClass;
    }
  }

  /**
   * Creates a list of the specified number of integers without duplication which
   * are randomly selected from the specified range.
   * @param num Number of the integers.
   * @param min Minimum value of selectable integer.
   * @param max Maximum value of selectable integer.
   * @return So generated list of integers.
   */
  private ArrayList _getRandomList(int num, int min, int max) {
    ArrayList list = new ArrayList();
    ArrayList tmp = new ArrayList();
    for (int i = min; i <= max; i++) {
      tmp.add(new Integer(i));
    }

    for (int i = 0; i < num; i++) {
      int pos = _getRandomFromRange(0, tmp.size() - 1);
      list.add( (Integer) tmp.get(pos));
      tmp.remove(pos);
    }

    return list;
  }

  /**
   * Randomly selects a integer from the specified range.
   * @param min Minimum value of the selectable integer.
   * @param max Maximum value of the selectable integer.
   * @return The selected integer.
   */
  private int _getRandomFromRange(int min, int max) {
    return min + random_.nextInt(max - min + 1);
  }
  
	/**
	 * Gets the axiomatic triples, dictionary encoded
	 * @return
	 */
	 public static ArrayList<Triple> getAxiomaticTriples() {
		  ArrayList<Triple> ret=new ArrayList<Triple>(axiomaticTriples.length);
		  for (long[] a:axiomaticTriples) {
			long s=a[0];
			long p=a[1];
			long o=a[2];
			boolean oIsLiteral= (o>=512) && (o&1)==1;
			ret.add(new Triple(s,p,o,oIsLiteral));
		  }
		  return ret;
	  }
  
  private static final long[][] axiomaticTriples={{810, 4, 812},
	  {13, 4, 5},
	  {14, 4, 4},
	  {0, 4, 0},
	  {3, 4, 3},
	  {2, 4, 2},
	  {4, 4, 4},
	  {5, 4, 5},
	  {0, 2, 23},
	  {3, 2, 23},
	  {2, 2, 23},
	  {4, 2, 1},
	  {5, 2, 23},
	  {558, 2, 814},
	  {516, 2, 20},
	  {520, 2, 20},
	  {816, 5, 818},
	  {820, 5, 818},
	  {822, 5, 818},
	  {21, 5, 1},
	  {824, 5, 20},
	  {22, 5, 23},
	  {3, 3, 1},
	  {2, 3, 1},
	  {4, 3, 1},
	  {5, 3, 23},
	  {826, 3, 828},
	  {830, 3, 828},
	  {832, 3, 828},
	  {550, 3, 814},
	  {558, 3, 814},
	  {0, 834, 5},
	  {0, 0, 24},
	  {24, 0, 24},
	  {1, 0, 24},
	  {4, 0, 24},
	  {11, 0, 24},
	  {5, 0, 24},
	  {9, 0, 24},
	  {10, 0, 24},
	  {14, 0, 24},
	  {13, 0, 24},
	  {0, 0, 1},
	  {826, 0, 1},
	  {830, 0, 1},
	  {832, 0, 1},
	  {550, 0, 1},
	  {558, 0, 1},
	  {836, 0, 1},
	  {562, 0, 814},
	  {824, 0, 22},
	  {13, 0, 10},
	  {13, 0, 9},
	  {14, 0, 10},
	  {14, 0, 9},
	  {11, 0, 9},
	  {12, 0, 9},
	  {5, 0, 10},
	  {4, 0, 10},
	  {838, 0, 9},
	  {840, 0, 22},
	  {842, 0, 22}};
 
  
 // The schema dictionary encoded
 private static final long[][] schema={{512, 0, 514},
	 {512, 0, 514},
	 {512, 516, 519},
	 {512, 520, 523},
	 {512, 524, 527},
	 {528, 0, 6},
	 {528, 520, 531},
	 {528, 5, 532},
	 {534, 0, 6},
	 {534, 520, 537},
	 {534, 5, 143},
	 {134, 0, 6},
	 {134, 520, 539},
	 {134, 5, 131},
	 {133, 0, 6},
	 {133, 520, 541},
	 {133, 5, 131},
	 {542, 0, 6},
	 {542, 520, 545},
	 {542, 5, 143},
	 {144, 0, 6},
	 {144, 520, 547},
	 {548, 550, 552},
	 {552, 0, 6},
	 {554, 550, 556},
	 {548, 558, 554},
	 {556, 0, 560},
	 {556, 16, 73},
	 {129, 0, 6},
	 {556, 17, 129},
	 {554, 558, 562},
	 {144, 564, 548},
	 {144, 5, 131},
	 {566, 0, 6},
	 {566, 520, 569},
	 {566, 5, 528},
	 {570, 0, 6},
	 {570, 520, 573},
	 {570, 5, 574},
	 {576, 0, 6},
	 {576, 520, 579},
	 {576, 5, 534},
	 {141, 0, 6},
	 {141, 520, 581},
	 {141, 5, 582},
	 {584, 0, 6},
	 {584, 520, 587},
	 {588, 550, 552},
	 {552, 0, 6},
	 {590, 550, 592},
	 {588, 558, 590},
	 {592, 0, 560},
	 {592, 16, 73},
	 {570, 0, 6},
	 {592, 17, 570},
	 {590, 558, 562},
	 {584, 564, 588},
	 {584, 5, 131},
	 {129, 0, 6},
	 {129, 520, 595},
	 {129, 5, 574},
	 {596, 0, 6},
	 {596, 520, 599},
	 {600, 550, 552},
	 {552, 0, 6},
	 {602, 550, 604},
	 {600, 558, 602},
	 {604, 0, 560},
	 {604, 16, 73},
	 {606, 0, 6},
	 {604, 17, 606},
	 {602, 558, 562},
	 {596, 564, 600},
	 {532, 0, 6},
	 {532, 520, 609},
	 {610, 550, 552},
	 {552, 0, 6},
	 {612, 550, 614},
	 {610, 558, 612},
	 {614, 0, 560},
	 {614, 16, 79},
	 {574, 0, 6},
	 {614, 17, 574},
	 {612, 558, 562},
	 {532, 564, 610},
	 {130, 0, 6},
	 {130, 520, 617},
	 {130, 5, 532},
	 {132, 0, 6},
	 {132, 520, 619},
	 {132, 5, 131},
	 {142, 0, 6},
	 {142, 520, 621},
	 {142, 5, 141},
	 {138, 0, 6},
	 {138, 520, 623},
	 {138, 5, 552},
	 {624, 0, 560},
	 {624, 16, 65},
	 {142, 0, 6},
	 {624, 17, 142},
	 {138, 5, 624},
	 {626, 0, 6},
	 {626, 520, 629},
	 {626, 5, 574},
	 {630, 0, 6},
	 {630, 520, 633},
	 {630, 5, 534},
	 {135, 0, 6},
	 {135, 520, 635},
	 {135, 5, 130},
	 {636, 0, 6},
	 {636, 520, 639},
	 {636, 5, 143},
	 {574, 0, 6},
	 {574, 520, 641},
	 {552, 0, 6},
	 {552, 520, 643},
	 {644, 0, 6},
	 {644, 520, 647},
	 {644, 5, 130},
	 {131, 0, 6},
	 {131, 520, 649},
	 {131, 5, 130},
	 {606, 0, 6},
	 {606, 520, 651},
	 {606, 5, 574},
	 {143, 0, 6},
	 {143, 520, 653},
	 {145, 0, 6},
	 {145, 520, 655},
	 {145, 5, 582},
	 {140, 0, 6},
	 {140, 520, 657},
	 {140, 5, 136},
	 {658, 0, 560},
	 {658, 16, 79},
	 {146, 0, 6},
	 {658, 17, 146},
	 {140, 5, 658},
	 {146, 0, 6},
	 {146, 520, 661},
	 {146, 5, 574},
	 {662, 0, 6},
	 {662, 520, 665},
	 {666, 0, 6},
	 {666, 520, 669},
	 {666, 5, 143},
	 {670, 0, 6},
	 {670, 520, 673},
	 {670, 5, 143},
	 {136, 0, 6},
	 {136, 520, 675},
	 {676, 550, 552},
	 {552, 0, 6},
	 {678, 550, 680},
	 {676, 558, 678},
	 {680, 0, 560},
	 {680, 16, 65},
	 {141, 0, 6},
	 {680, 17, 141},
	 {678, 558, 562},
	 {136, 564, 676},
	 {682, 0, 6},
	 {682, 520, 685},
	 {682, 5, 528},
	 {139, 0, 6},
	 {139, 520, 687},
	 {688, 550, 552},
	 {552, 0, 6},
	 {690, 550, 692},
	 {688, 558, 690},
	 {692, 0, 560},
	 {692, 16, 74},
	 {141, 0, 6},
	 {692, 17, 141},
	 {690, 558, 562},
	 {139, 564, 688},
	 {694, 0, 6},
	 {694, 520, 697},
	 {694, 5, 534},
	 {137, 0, 6},
	 {137, 520, 699},
	 {137, 5, 136},
	 {128, 0, 6},
	 {128, 520, 701},
	 {128, 5, 574},
	 {702, 0, 6},
	 {702, 520, 705},
	 {702, 5, 143},
	 {706, 0, 6},
	 {706, 520, 709},
	 {706, 5, 131},
	 {582, 0, 6},
	 {582, 520, 711},
	 {70, 0, 712},
	 {70, 520, 715},
	 {70, 3, 552},
	 {70, 2, 131},
	 {716, 0, 712},
	 {716, 520, 719},
	 {716, 3, 574},
	 {716, 2, 574},
	 {720, 0, 712},
	 {720, 520, 719},
	 {720, 3, 574},
	 {720, 2, 552},
	 {722, 0, 724},
	 {722, 520, 727},
	 {722, 3, 552},
	 {728, 0, 712},
	 {728, 520, 731},
	 {728, 3, 552},
	 {728, 2, 128},
	 {728, 12, 732},
	 {69, 0, 712},
	 {69, 520, 735},
	 {69, 3, 552},
	 {69, 2, 128},
	 {69, 4, 728},
	 {76, 0, 724},
	 {76, 520, 737},
	 {76, 3, 552},
	 {732, 0, 712},
	 {732, 520, 739},
	 {732, 3, 128},
	 {732, 2, 552},
	 {732, 12, 728},
	 {73, 0, 712},
	 {73, 520, 741},
	 {73, 4, 79},
	 {742, 0, 712},
	 {742, 520, 745},
	 {742, 3, 662},
	 {742, 2, 141},
	 {68, 0, 712},
	 {68, 520, 747},
	 {68, 3, 552},
	 {68, 2, 128},
	 {68, 4, 728},
	 {748, 0, 712},
	 {748, 520, 751},
	 {748, 3, 574},
	 {748, 2, 552},
	 {71, 0, 712},
	 {71, 520, 753},
	 {71, 12, 748},
	 {64, 0, 724},
	 {64, 520, 755},
	 {756, 0, 724},
	 {756, 520, 759},
	 {760, 0, 712},
	 {760, 520, 763},
	 {760, 3, 574},
	 {760, 2, 143},
	 {72, 0, 712},
	 {72, 520, 765},
	 {72, 3, 143},
	 {72, 2, 552},
	 {766, 0, 712},
	 {766, 520, 769},
	 {766, 3, 143},
	 {770, 0, 712},
	 {770, 520, 773},
	 {770, 3, 143},
	 {770, 2, 145},
	 {75, 0, 724},
	 {75, 520, 775},
	 {776, 0, 712},
	 {776, 520, 779},
	 {776, 3, 146},
	 {776, 2, 145},
	 {780, 0, 712},
	 {780, 520, 783},
	 {780, 3, 666},
	 {780, 2, 143},
	 {784, 0, 712},
	 {784, 520, 787},
	 {784, 3, 666},
	 {78, 0, 10},
	 {78, 520, 789},
	 {78, 3, 574},
	 {78, 2, 574},
	 {65, 0, 712},
	 {65, 520, 791},
	 {66, 0, 712},
	 {66, 520, 793},
	 {66, 3, 130},
	 {66, 2, 141},
	 {74, 0, 712},
	 {74, 520, 795},
	 {74, 3, 139},
	 {74, 2, 141},
	 {77, 0, 724},
	 {77, 520, 797},
	 {77, 3, 552},
	 {798, 0, 712},
	 {798, 520, 801},
	 {798, 3, 131},
	 {802, 0, 724},
	 {802, 520, 805},
	 {802, 3, 552},
	 {67, 0, 712},
	 {67, 520, 807},
	 {67, 3, 552},
	 {67, 2, 128},
	 {67, 4, 728},
	 {79, 0, 712},
	 {79, 520, 809},
	 {79, 4, 71}};

}