package de.uni_mannheim.informatik.dws.t2k.match;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;

import au.com.bytecode.opencsv.CSVWriter;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.goldstandard.GoldStandard;
import de.uni_mannheim.informatik.dws.t2k.index.IIndex;
import de.uni_mannheim.informatik.dws.t2k.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.t2k.index.io.InMemoryIndex;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowDateComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.ExtractedTriple;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTable;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTableKeyToRdfsLabelCorrespondenceGenerator;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.cli.Executable;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.java.BuildInfo;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.Performance;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.parallel.ParallelMatchingEngine;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.aggregators.CountAggregator;
import de.uni_mannheim.informatik.wdi.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.wdi.similarity.numeric.PercentageSimilarity;
import de.uni_mannheim.informatik.wdi.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.wdi.spark.SparkMatchingEngine;

public class T2KMatch extends Executable implements Serializable {
	
		// (1) load x tables and put all rows in a dataset (+table identifier)
		// (2) load dbp
		
		// (3) run candidate selection on tables dataset (+add candidate attribute)
		
		// (4) create initial schema mapping (empty?)
		
		// (5) match tables+candidate dataset by join (blocking) with dbp dataset
		// re-calculate candidate score using schema mapping as weights (if empty skip)
		// create schema mapping per record
		
		// (6) group results by table (new component for wdi framework ... aggregation step in matching rule?)
		// aggregate schema mapping on table level using candidate scores as weights
		
		// (7) go back to (5) for iteration
	
	private static final long serialVersionUID = 1L;

	@Parameter(names = "-sf")
	private String sfLocation;
	
	@Parameter(names = "-kb", required=true)
	private String kbLocation;
	
	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	@Parameter(names = "-identityGS")
	private String identityGSLocation;
	
	@Parameter(names = "-schemaGS")
	private String schemaGSLocation;
	
	@Parameter(names = "-classGS")
	private String classGSLocation;
	
	@Parameter(names = "-index")
	private String indexLocation;
	
	@Parameter(names = "-measureMemory")
	private boolean measure;
	
	@Parameter(names = "-sparkMaster")
	private String sparkMaster;
	
	@Parameter(names = "-sparkJar")
	private String sparkJar;
	
	@Parameter(names = "-results", required=true)
	private String resultLocation;
	
	@Parameter(names = "-ontology", required=true)
	private String ontologyLocation;
	
	@Parameter(names = "-readGS")
	private String readGSLocation;
	
	@Parameter(names = "-writeGS")
	private String writeGSLocation;

	@Parameter(names = "-rd")
	private String rdLocation;

	@Parameter(names = "-verbose")
	private boolean verbose = false;
	
	@Parameter(names = "-detectKeys")
	private boolean detectKeys;
	
	/*******
	 * Parameters for algorithm configuration
	 *******/
	@Parameter(names = "-mappedRatio")
	private double par_mappedRatio=0.0;
	
	
    public static void main( String[] args ) throws Exception
    {
    	T2KMatch t2k = new T2KMatch();
    	
    	if(t2k.parseCommandLine(T2KMatch.class, args)) {
    		
    		t2k.initialise();
    		
    		t2k.match();
    		
    	}
    }
    
    private IIndex index;
    private KnowledgeBase kb;
    private WebTables web;
    private MatchingGoldStandard instanceGs;
    private MatchingGoldStandard schemaGs;
    private MatchingGoldStandard classGs;
    private SurfaceForms sf;
    private File results;
    
    public void initialise() throws IOException {	 
    	if(sfLocation==null && rdLocation==null){
    		sf = new SurfaceForms(null, null);
    	}else if(sfLocation==null && rdLocation!=null){
    		sf = new SurfaceForms(null, new File(rdLocation));
    	}else if(sfLocation!=null && rdLocation==null){
    		sf = new SurfaceForms(new File(sfLocation), null);
    	}else{
    		sf = new SurfaceForms(new File(sfLocation), new File(rdLocation));
    	}
    	
    	boolean createIndex = false;
    	// create index for candidate lookup
    	if(indexLocation==null) {
    		// no index provided, create a new one in memory
    		index = new InMemoryIndex();
    		createIndex = true;
    	} else{
    		// load index from location that was provided
    		index = new DefaultIndex(indexLocation);
    		createIndex = !new File(indexLocation).exists();
    	}
    	if(createIndex) {
    		sf.loadIfRequired();
    	}
    	
    	//first load DBpedia class Hierarchy
    	KnowledgeBase.loadClassHierarchy(ontologyLocation);
    	
    	// load knowledge base, fill index if it is empty
		kb = KnowledgeBase.loadKnowledgeBase(new File(kbLocation), createIndex?index:null, measure, sf);

    	// load instance gold standard
    	if(identityGSLocation!=null) {
	    	File instGsFile = new File(identityGSLocation);
	    	if(instGsFile.exists()) {
		    	instanceGs = new MatchingGoldStandard();
		    	instanceGs.loadFromCSVFile(instGsFile);
		    	instanceGs.setComplete(true);
	    	}
    	}
    	
    	// load schema gold standard
    	if(schemaGSLocation!=null) {
    		File schemaGsFile = new File(schemaGSLocation);
    		if(schemaGsFile.exists()) {
				schemaGs = new MatchingGoldStandard();
				schemaGs.loadFromCSVFile(schemaGsFile);
				schemaGs.setComplete(true);
    		}
    	}

    	// load class gold standard
    	if(classGSLocation!=null) {
    		File classGsFile = new File(classGSLocation);
    		if(classGsFile.exists()) {
				classGs = new MatchingGoldStandard();
				classGs.loadFromCSVFile(classGsFile);
				classGs.setComplete(true);
    		}
    	}
    	
    	//TODO Sani: This should be a separate program, create a new class with its own main() 
    	if(readGSLocation != null && writeGSLocation != null){
	    	//create an instances gold standard for evaluation purposes. If you create new instance gold standard then don not forget to load new instance gold standard
	    	GoldStandard gs = new GoldStandard(readGSLocation, writeGSLocation, web);
	    	gs.convertOldGStoNewGS();
	    	
	    	// load new instance gold standard instead of old gold standard
	    	if(writeGSLocation!=null) {
		    	File instGsFile = new File(writeGSLocation);
		    	if(instGsFile.exists()) {
		    		instanceGs = new MatchingGoldStandard();
			    	instanceGs.loadFromCSVFile(instGsFile);
			    	instanceGs.setComplete(true);
		    	}
	    	}
    	}
    	
    	if(sparkJar==null) {
    		sparkJar = BuildInfo.getJarPath(this.getClass()).getAbsolutePath();
    	}
    	
    	results = new File(resultLocation);
    	if(!results.exists()) {
    		results.mkdirs();
    	}
    }
    
    public void match() throws Exception {
    	/***********************************************
    	 * Matching Framework Initialisation
    	 ***********************************************/
    	// create matching engine
//    	MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine = new MatchingEngine<>();
    	MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine = new ParallelMatchingEngine<>();
    	
    	
    	/***********************************************
    	 * Spark-Specific Initialisation
    	 ***********************************************/
    	if(sparkMaster!=null) {
	    	JavaSparkContext sc = SparkMatchingEngine.createSparkContextCluster("T2Kv2", sparkMaster, "300g", "21", sparkJar);
	    	
	    	SparkMatchingEngine<MatchableTableRow, MatchableTableColumn> sparkEngine = new SparkMatchingEngine<>(sc);
	    	matchingEngine = sparkEngine;
	    	
	    	// kb is already loaded locally (needed to calculate class weights at the moment)
	    	// however, we must load it again on the cluster
	    	kb.load(sparkEngine.loadTextFiles(kbLocation, 42), matchingEngine.getProcessingEngine());
	    	web = new WebTables();
	    	web.load(sparkEngine.loadTextFiles(webLocation, 42), matchingEngine.getProcessingEngine());
    	} else {
    		web = WebTables.loadWebTables(new File(webLocation), false, true, detectKeys);
    	}
    	
    	DataProcessingEngine dataEngine = matchingEngine.getProcessingEngine();

    	/***********************************************
    	 * Gold Standard Adjustment
    	 ***********************************************/
    	// remove all correspondences from the GS for tables that were not loaded
    	if(instanceGs!=null) {
    		instanceGs.removeNonexistingExamples(web.getRecords());
    	}
    	if(schemaGs!=null) {
    		schemaGs.removeNonexistingExamples(web.getSchema());
    	}
    	if(classGs!=null) {
    		classGs.removeNonexistingExamples(web.getTables());
    	}
    	
    	
    	/***********************************************
    	 * Key Preparation
    	 ***********************************************/
    	// create schema correspondences between the key columns and rdfs:Label
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences = dataEngine.transform(web.getKeys(), new WebTableKeyToRdfsLabelCorrespondenceGenerator(kb.getRdfsLabel()));
    	if(verbose) {
    		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : keyCorrespondences.get()) {
    			System.out.println(String.format("%s: [%d]%s", web.getTableNames().get(cor.getFirstRecord().getTableId()), cor.getFirstRecord().getColumnIndex(), cor.getFirstRecord().getHeader()));
    		}
    	}
    	
    	/***********************************************
    	 * Candidate Selection
    	 ***********************************************/
    	MatchingLogger.printHeader("Candidate Selection");
    	CandidateSelection cs = new CandidateSelection(matchingEngine, sparkMaster!=null, index, indexLocation, web, kb, sf, keyCorrespondences);
    	ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = cs.run();
    	evaluateInstanceCorrespondences(instanceCorrespondences, "candidate");
    	if(verbose) {
    		printCandidateStatistics(instanceCorrespondences, dataEngine);
    	}
    	
    	/***********************************************
    	 *Candidate Selection - Class Decision
    	 ***********************************************/
    	MatchingLogger.printHeader("Candidate Selection - Class Decision");
    	ClassDecision classDec = new ClassDecision();
    	// classesPerTable does not contain the super classes of the selected classes
    	Map<Integer, Set<String>> classesPerTable = classDec.runClassDecision(kb, instanceCorrespondences, dataEngine);
    	evaluateClassCorrespondences(createClassCorrespondences(classesPerTable), "instance-based");
    	
    	/***********************************************
    	 *Candidate Selection - Candidate Refinement
    	 ***********************************************/
    	MatchingLogger.printHeader("Candidate Selection - Candidate Refinement");
    	CandidateRefinement cr = new CandidateRefinement(matchingEngine, sparkMaster!=null, index, indexLocation, web, kb, sf, keyCorrespondences, classesPerTable);
    	instanceCorrespondences = cr.run();
    	evaluateInstanceCorrespondences(instanceCorrespondences, "refined candidate");
    	if(verbose) {
    		printCandidateStatistics(instanceCorrespondences, dataEngine);
    	}
   
    	/***********************************************
    	 *Candidate Selection - Property-based Class Refinement
    	 ***********************************************/
    	MatchingLogger.printHeader("Property-based Class Refinement");
    	// match properties
    	DuplicateBasedSchemaMatching schemaMatchingForClassRefinement = new DuplicateBasedSchemaMatching(matchingEngine, web, kb, sf, classesPerTable, instanceCorrespondences, false);
    	schemaMatchingForClassRefinement.setFinalPropertySimilarityThreshold(0.03);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = schemaMatchingForClassRefinement.run();
    	// add key correspondences (some tables only have key correspondences)
    	schemaCorrespondences = dataEngine.append(schemaCorrespondences, keyCorrespondences);
    	evaluateSchemaCorrespondences(schemaCorrespondences, "duplicate-based for refinement");
    	// determine most probable class mapping
    	ClassRefinement classRefinement = new ClassRefinement(matchingEngine, kb.getPropertyIndices(), KnowledgeBase.getClassHierarchy(),schemaCorrespondences,classesPerTable, kb.getClassIds());
    	classesPerTable = classRefinement.run();
    	Map<Integer, String> finalClassPerTable = classRefinement.getFinalClassPerTable();
//    	evaluateClassCorrespondences(createClassCorrespondences(classesPerTable), "schema-based");
    	evaluateClassCorrespondences(createClassCorrespondence(finalClassPerTable), "schema-based");
    	
    	/***********************************************
    	 *Candidate Selection - Class-based Filtering
    	 ***********************************************/
    	CandidateFiltering classFilter = new CandidateFiltering(matchingEngine, classesPerTable, kb.getClassIndices(), instanceCorrespondences);
    	instanceCorrespondences = classFilter.run();
    	evaluateInstanceCorrespondences(instanceCorrespondences, "property refined candidate");
    	if(verbose) {
    		printCandidateStatistics(instanceCorrespondences, dataEngine);
    	}
    	
    	/***********************************************
    	 *Iterative Matching
    	 ***********************************************/
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> labelBasedSchemaCorrespondences = null;
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> lastSchemaCorrespondences = null;
    	
    	LabelBasedSchemaMatching labelBasedSchema = new LabelBasedSchemaMatching(matchingEngine, web, kb, classesPerTable, instanceCorrespondences);
    	DuplicateBasedSchemaMatching duplicateBasedSchema = new DuplicateBasedSchemaMatching(matchingEngine, web, kb, sf, classesPerTable, instanceCorrespondences, false);
    	CombineSchemaCorrespondences combineSchema = new CombineSchemaCorrespondences(matchingEngine, keyCorrespondences);
    	combineSchema.setVerbose(verbose);
    	IdentityResolution identityResolution = new IdentityResolution(matchingEngine, web, kb, sf);
    	UpdateSchemaCorrespondences updateSchema = new UpdateSchemaCorrespondences(matchingEngine);
    	
    	int iteration = 0;
    	do { // iterative matching loop
    		/***********************************************
	    	 * Schema Matching - Label Based
	    	 ***********************************************/
    		MatchingLogger.printHeader("Schema Matching - Label Based");
    		labelBasedSchema.setInstanceCorrespondences(instanceCorrespondences);
    		labelBasedSchemaCorrespondences = labelBasedSchema.run();
    		evaluateSchemaCorrespondences(labelBasedSchemaCorrespondences, "label-based");
    		
	    	/***********************************************
	    	 * Schema Matching - Duplicate Based
	    	 ***********************************************/
	    	MatchingLogger.printHeader("Schema Matching - Duplicate Based");
	    	duplicateBasedSchema.setInstanceCorrespondences(instanceCorrespondences);
	    	schemaCorrespondences = duplicateBasedSchema.run();
	    	evaluateSchemaCorrespondences(schemaCorrespondences, "duplicate-based");
	    	
	    	/***********************************************
	    	 * Combine Schema Correspondences
	    	 ***********************************************/
	    	MatchingLogger.printHeader("Combine Schema Correspondences");
	    	combineSchema.setSchemaCorrespondences(schemaCorrespondences);
	    	combineSchema.setLabelBasedSchemaCorrespondences(labelBasedSchemaCorrespondences);
	    	schemaCorrespondences = combineSchema.run();
	    	evaluateSchemaCorrespondences(schemaCorrespondences, "combined");

	    	/***********************************************
	    	 * Iterative - Update Schema Correspondences
	    	 ***********************************************/
	    	if(lastSchemaCorrespondences!=null) {
	    		updateSchema.setSchemaCorrespondences(lastSchemaCorrespondences);
	    		updateSchema.setNewSchemaCorrespondences(schemaCorrespondences);
	    		schemaCorrespondences = updateSchema.run();
	    		evaluateSchemaCorrespondences(schemaCorrespondences, "updated");
	    	}
	    	
	    	/***********************************************
	    	 * Identity Resolution
	    	 ***********************************************/
	    	MatchingLogger.printHeader("Identity Resolution");
	    	identityResolution.setInstanceCorrespondences(instanceCorrespondences);
	    	identityResolution.setSchemaCorrespondences(schemaCorrespondences);
	    	instanceCorrespondences = identityResolution.run();
	    	evaluateInstanceCorrespondences(instanceCorrespondences, "final");
	    	if(verbose) {
	    		printCandidateStatistics(instanceCorrespondences, dataEngine);
	    	}

    	
	    	lastSchemaCorrespondences = schemaCorrespondences;
    	} while(++iteration<1); // loop for iterative part
    	
    	/***********************************************
    	 * One-to-one Matching
    	 ***********************************************/
    	instanceCorrespondences = TopKMatch.getTopKMatch(instanceCorrespondences, dataEngine, 1, 0.0);
    	schemaCorrespondences = TopKMatch.getTopKMatch(schemaCorrespondences, dataEngine, 1, 0.0);

    	/***********************************************
    	 *Table Filtering - Mapped Ratio Filter
    	 ***********************************************/
    	if(par_mappedRatio>0.0) {
	    	// current makes the result worse ...
	    	TableFiltering tableFilter = new TableFiltering(matchingEngine, web, instanceCorrespondences, classesPerTable, schemaCorrespondences);
	    	tableFilter.setMinMappedRatio(par_mappedRatio);
	    	tableFilter.run();
	    	classesPerTable = tableFilter.getClassesPerTable();
	    	instanceCorrespondences = tableFilter.getInstanceCorrespondences();
	    	schemaCorrespondences = tableFilter.getSchemaCorrespondences();
    	}
    	
    	/***********************************************
    	 * Evaluation
    	 ***********************************************/
    	evaluateInstanceCorrespondences(instanceCorrespondences, "");
    	evaluateSchemaCorrespondences(schemaCorrespondences, "");
		evaluateClassCorrespondences(createClassCorrespondence(finalClassPerTable), "");
		
    	/***********************************************
    	 * Write Results
    	 ***********************************************/
		matchingEngine.writeCorrespondences(instanceCorrespondences.get(), new File(results, "instannce_correspondences.csv"));
		matchingEngine.writeCorrespondences(schemaCorrespondences.get(), new File(results, "schema_correspondences.csv"));
		
    	HashMap<Integer, String> inverseTableIndices = (HashMap<Integer, String>) MapUtils.invert(web.getTableIndices());
		CSVWriter csvWriter = new CSVWriter(new FileWriter(new File(results, "class_decision.csv")));
		for(Integer tableId : classesPerTable.keySet()) {
			csvWriter.writeNext(new String[] {tableId.toString(), inverseTableIndices.get(tableId), classesPerTable.get(tableId).toString().replaceAll("\\[", "").replaceAll("\\]", "")});
		}
		csvWriter.close();
		
		TripleGenerator tripleGen = new TripleGenerator(web, kb, dataEngine);
		tripleGen.setComparatorForType(DataType.string, new MatchableTableRowComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), kb.getPropertyIndices(), 0.5, sf, true));
		tripleGen.setComparatorForType(DataType.numeric, new MatchableTableRowComparator<>(new PercentageSimilarity(0.05), kb.getPropertyIndices(), 0.00));
//		tripleGen.setComparatorForType(DataType.date, new MatchableTableRowComparator<>(new WeightedDateSimilarity(1, 1, 1), kb.getPropertyIndices(), 1.0));
		tripleGen.setComparatorForType(DataType.date, new MatchableTableRowDateComparator(new WeightedDateSimilarity(1, 3, 5), kb.getPropertyIndices(), 0.9));
		ResultSet<ExtractedTriple> triples = tripleGen.run(instanceCorrespondences, schemaCorrespondences);
		System.out.println(String.format("Extracted %d existing (%.4f%% correct) and %d new triples!", tripleGen.getExistingTripleCount(), tripleGen.getCorrectTripleCount()*100.0/(double)tripleGen.getExistingTripleCount(), tripleGen.getNewTripleCount()));
		ExtractedTriple.writeCSV(new File(results, "extracted_triples.csv"), triples.get());
		
		//TODO add the correspondences to the tables and write them to the disk
		
    	/***********************************************
    	 * Shutdown Spark Context
    	 ***********************************************/
		if(sparkMaster!=null) {
			((SparkMatchingEngine<MatchableTableRow, MatchableTableColumn>)matchingEngine).shutdown();
//			sparkEngine.shutdown();
		}
    }
	
    protected void evaluateInstanceCorrespondences(ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, String name) {
    	Performance instancePerf = null;
    	if(instanceGs!=null) {
    		instanceCorrespondences.deduplicate();
	    	MatchingEvaluator<MatchableTableRow, MatchableTableColumn> instanceEvaluator = new MatchingEvaluator<>(false);
	    	Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondencesCollection = instanceCorrespondences.get();
	    	System.out.println(String.format("%d %s instance correspondences", instanceCorrespondencesCollection.size(), name));
	    	instancePerf = instanceEvaluator.evaluateMatching(instanceCorrespondencesCollection, instanceGs);
    	}

		if(instancePerf!=null) {
			System.out
			.println(String.format(
					"Instance Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
					instancePerf.getPrecision(), instancePerf.getRecall(),
					instancePerf.getF1()));
		}
    }
    
    protected void evaluateSchemaCorrespondences(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, String name) {
    	Performance schemaPerf = null;
		if(schemaGs!=null) {
			schemaCorrespondences.deduplicate();
			MatchingEvaluator<MatchableTableColumn, MatchableTableRow> schemaEvaluator = new MatchingEvaluator<>(false);
			Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondencesCollection = schemaCorrespondences.get();
			System.out.println(String.format("%d %s schema correspondences", schemaCorrespondencesCollection.size(), name));
			schemaPerf = schemaEvaluator.evaluateMatching(schemaCorrespondencesCollection, schemaGs);
		}
		
		if(schemaPerf!=null) {
			System.out
			.println(String.format(
					"Schema Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
					schemaPerf.getPrecision(), schemaPerf.getRecall(),
					schemaPerf.getF1()));
		}	
    }
    
    protected void evaluateClassCorrespondences(ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondences, String name) {
    	Performance classPerf = null;
		if(classGs!=null) {
			classCorrespondences.deduplicate();
			MatchingEvaluator<MatchableTable, MatchableTableColumn> classEvaluator = new MatchingEvaluator<>(false);
			Collection<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondencesCollection = classCorrespondences.get();
			System.out.println(String.format("%d %s class correspondences", classCorrespondencesCollection.size(), name));
			classPerf = classEvaluator.evaluateMatching(classCorrespondencesCollection, classGs);
		}
		
		if(classPerf!=null) {
			System.out
			.println(String.format(
					"Class Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
					classPerf.getPrecision(), classPerf.getRecall(),
					classPerf.getF1()));
		}
    }
    
    protected ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> createClassCorrespondences(Map<Integer, Set<String>> classesPerTable) {
    	//TODO the class matching should be replaced by actual matchers that create correspondences, such that we don't need this method
    	ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> result = new ResultSet<>();
    	
    	for(int tableId : classesPerTable.keySet()) {
    		
    		MatchableTable webTable = web.getTables().getRecord(web.getTableNames().get(tableId));
    		
    		for(String className : classesPerTable.get(tableId)) {
    			
    			MatchableTable kbTable = kb.getTables().getRecord(className);

    			Correspondence<MatchableTable, MatchableTableColumn> cor = new Correspondence<MatchableTable, MatchableTableColumn>(webTable, kbTable, 1.0, null);
    			result.add(cor);
    		}
    		
    	}
    	
    	return result;
    }
    protected ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> createClassCorrespondence(Map<Integer, String> classPerTable) {
    	//TODO the class matching should be replaced by actual matchers that create correspondences, such that we don't need this method
    	ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> result = new ResultSet<>();
    	
    	for(int tableId : classPerTable.keySet()) {
    		
    		MatchableTable webTable = web.getTables().getRecord(web.getTableNames().get(tableId));
    		
    		String className = classPerTable.get(tableId);
    			
			MatchableTable kbTable = kb.getTables().getRecord(className);

			Correspondence<MatchableTable, MatchableTableColumn> cor = new Correspondence<MatchableTable, MatchableTableColumn>(webTable, kbTable, 1.0, null);
			result.add(cor);
    		
    	}
    	
    	return result;
    }
    
    protected void printCandidateStatistics(ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, DataProcessingEngine proc) {
    	
    	RecordKeyValueMapper<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>> groupBy = new RecordKeyValueMapper<String, Correspondence<MatchableTableRow,MatchableTableColumn>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableColumn> record,
					DatasetIterator<Pair<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {
				
				String tableName = web.getTableNames().get(record.getFirstRecord().getTableId());
				
				resultCollector.next(new Pair<String, Correspondence<MatchableTableRow,MatchableTableColumn>>(tableName, record));
				
			}
		};
		ResultSet<Pair<String, Integer>> counts = proc.aggregateRecords(instanceCorrespondences, groupBy, new CountAggregator<String, Correspondence<MatchableTableRow, MatchableTableColumn>>());
		
		// get class distribution
		DataAggregator<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Map<String, Integer>> classAggregator = new DataAggregator<String, Correspondence<MatchableTableRow,MatchableTableColumn>, Map<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, Integer> initialise(String keyValue) {
				return new HashMap<>();
			}
			
			@Override
			public Map<String, Integer> aggregate(Map<String, Integer> previousResult,
					Correspondence<MatchableTableRow, MatchableTableColumn> record) {
				
				String className = kb.getClassIndices().get(record.getSecondRecord().getTableId()); 
				
				Integer cnt = previousResult.get(className);
				if(cnt==null) {
					cnt = 0;
				}
				
				previousResult.put(className, cnt+1);
				
				return previousResult;
			}
		};
		
		ResultSet<Pair<String, Map<String, Integer>>> classDistribution = proc.aggregateRecords(instanceCorrespondences, groupBy, classAggregator);
		final Map<String, Map<String, Integer>> classesByTable = Pair.toMap(classDistribution.get());
		
		System.out.println("Candidates per Table:");
		for(final Pair<String, Integer> p : counts.get()) {
			System.out.println(String.format("\t%s\t%d", p.getFirst(), p.getSecond()));
			
			Collection<Pair<String, Integer>> classCounts = Q.sort(Pair.fromMap(classesByTable.get(p.getFirst())), new Comparator<Pair<String, Integer>>() {

				@Override
				public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
					return -Integer.compare(o1.getSecond(), o2.getSecond());
				}
			});
			
			System.out.println(String.format("\t\t%s", StringUtils.join(Q.project(classCounts, new Func<String, Pair<String, Integer>>() {

				@Override
				public String invoke(Pair<String, Integer> in) {
					return String.format("%s: %.4f%%", in.getFirst(), in.getSecond()*100.0/(double)p.getSecond());
				}
				
			}), ", ")));
		}
    	
    }
}
