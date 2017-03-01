/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.tnt.match.tasks;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DurationFormatUtils;

import com.beust.jcommander.Parameter;
import com.mysql.fabric.xmlrpc.base.Array;

import check_if_useful.MatchingKeyGenerator;
import check_if_useful.WebTablesMatchingGraph;
import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils2;
import de.uni_mannheim.informatik.dws.t2k.utils.data.graph.Graph;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TableReconstructor;
import de.uni_mannheim.informatik.dws.tnt.match.TableReport;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.tnt.match.blocking.KeyBasedBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.blocking.WebTableKeyBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.blocking.KeyBasedBlockingKeyGenerator.AmbiguityAvoidance;
import de.uni_mannheim.informatik.dws.tnt.match.blocking.KeyValueBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRowWithKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableMatchingKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandardCreator;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.DeterminantMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.JoinBasedIdentityResolution;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.SchemaFreeIdentityResolution;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.TableToTableMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.rules.CandidateKeyConsolidator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.SchemaSynonymBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.ValueBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceByKeyToSchemaAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceToSchemaAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceTransitivityAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.KeyPropagationAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaSynonymAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaToInstanceAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaToKeyKeyAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaTransitivityAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.TrustedKeyAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyBasedBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyBasedBlockingFunction;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyBySchemaCorrespondenceBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyCorrespondenceBasedBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlockerImproved2;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.SynonymBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.graph.ResolveConflictsByEdgeBetweenness;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.CorrespondenceInverter;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.DisjointHeaderSchemaMatchingRule;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.DuplicateBasedWebTableSchemaMatchingRule;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.KeyBasedRecordMatchingRule;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.KeyMatchingRefiner;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.RecordMatchingForSchemaVotingRule;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.SpecialColumnsSchemaFilter;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.matching.blocking.MultiKeyBlocker;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.Performance;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;
import de.uni_mannheim.informatik.wdi.parallel.ParallelDataProcessingEngine;
import de.uni_mannheim.informatik.wdi.parallel.ParallelMatchingEngine;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;
import de.uni_mannheim.informatik.wdi.processing.aggregators.CountAggregator;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableJoinUnion_bak_before_cleanup extends TnTTask {

	@Parameter(names = "-web")
	private String webLocation;
	/**
	 * @param webLocation the webLocation to set
	 */
	public void setWebLocation(String webLocation) {
		this.webLocation = webLocation;
	}
	
	@Parameter(names = "-results")
	private String resultLocation;
	/**
	 * @param resultLocation the resultLocation to set
	 */
	public void setResultLocation(String resultLocation) {
		this.resultLocation = resultLocation;
	}
	
	@Parameter(names = "-serialise")
	private boolean serialise;
	/**
	 * @param serialise the serialise to set
	 */
	public void setSerialise(boolean serialise) {
		this.serialise = serialise;
	}	
	
	public static void main(String[] args) throws Exception {
		TableJoinUnion_bak_before_cleanup tju = new TableJoinUnion_bak_before_cleanup();

		if (tju.parseCommandLine(TableJoinUnion_bak_before_cleanup.class, args)) {

			hello();

			tju.initialise();
//			tju.setDataProcessingEngine(new ParallelDataProcessingEngine());
//			tju.setMatchingEngine(new MatchingEngine<MatchableTableRow, MatchableTableColumn>());
			tju.setMatchingEngine(new ParallelMatchingEngine<MatchableTableRow, MatchableTableColumn>());
			tju.setDataProcessingEngine(tju.matchingEngine.getProcessingEngine());
			
			tju.match();

		}
	}

	private WebTables web;
	private File resultsLocationFile;
	private File csvResultsLocationFile;
	private File resultsLocationFileFuzzy;
	private File csvResultsLocationFileFuzzy;
	private File resultsLocationFileFuzzySubKey;
	private File csvResultsLocationFileFuzzySubKey;
	private File evaluationLocation;
	
	public void initialise() throws IOException {
		printHeadline("Table Join Union");
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		
		// prepare output directories
    	resultsLocationFile = new File(new File(resultLocation), "join_union");
    	setOutputDirectory(resultsLocationFile);
    	resultsLocationFile.mkdirs();
    	
    	csvResultsLocationFile = new File(new File(resultLocation), "join_union_csv");
    	csvResultsLocationFile.mkdirs();

    	resultsLocationFileFuzzy = new File(new File(resultLocation), "join_union_fuzzy");
    	setOutputDirectory(resultsLocationFileFuzzy);
    	resultsLocationFileFuzzy.mkdirs();
    	
    	csvResultsLocationFileFuzzy = new File(new File(resultLocation), "join_union_fuzzy_csv");
    	csvResultsLocationFileFuzzy.mkdirs();

    	resultsLocationFileFuzzySubKey = new File(new File(resultLocation), "join_union_fuzzy_subkey_json");
    	resultsLocationFileFuzzySubKey.mkdirs();
    	
    	csvResultsLocationFileFuzzySubKey = new File(new File(resultLocation), "join_union_fuzzy_subkey_csv");
    	csvResultsLocationFileFuzzySubKey.mkdirs();
    	
    	evaluationLocation = new File(new File(resultLocation), "evaluation");
    	evaluationLocation.mkdirs();
    	
    	// prepare gold standard
    	N2NGoldStandardCreator gsc = new N2NGoldStandardCreator();
    	gsc.createFromMappedUnionTables(web.getTables().values(), evaluationLocation);
	}

	public void match() throws Exception {

    	/***********************************************
    	 * Schema Statistics
    	 ***********************************************/
    	// maps a table id to its schema
    	final HashMap<Integer, String> tableToSchema = new HashMap<>();
    	// maps a table id to its candidate keys
    	final HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys = new HashMap<>();
    	ResultSet<WebTableKey> tableKeys = new ResultSet<>();
    	// maps a schema to a representative table
    	HashMap<String, Table> schemaToTable = new HashMap<>();
    	
    	Map<String, Set<String>> disjointHeaders = new HashMap<>();
    	TableSchemaStatistics stat = new TableSchemaStatistics();
    	for(Table t : web.getTables().values()) {
//    		t.clearProvenance();
    		
    		stat.addTable(t);
    		String schema = stat.generateSchemaString(t);
    		tableToSchema.put(web.getTableIndices().get(t.getPath()), schema);
    		if(!schemaToTable.containsKey(schema)) {
    			schemaToTable.put(schema, t);
    		} 
    		
    		ArrayList<Collection<Integer>> candKeys = new ArrayList<>(t.getSchema().getCandidateKeys().size());
    		for(Collection<TableColumn> key : t.getSchema().getCandidateKeys()) {
    			Collection<Integer> indices = Q.project(key, new TableColumn.ColumnIndexProjection());
    			candKeys.add(indices);
    			tableKeys.add(new WebTableKey(t.getTableId(), new HashSet<>(indices)));
    		}
    		tableToCandidateKeys.put(web.getTableIndices().get(t.getPath()), candKeys);
    		
    		for(int i = 0; i < t.getColumns().size(); i++) {
    			TableColumn c1 = t.getSchema().get(i);
    			
    			if(!c1.getHeader().equals("null") && !c1.getHeader().isEmpty()) {
    				Set<String> disjoint = MapUtils.get(disjointHeaders, c1.getHeader(), new HashSet<String>());
    				
	    			for(int j = 0; j < t.getColumns().size(); j++) {
	    				TableColumn c2 = t.getSchema().get(j);
	    				if(i!=j && !c2.getHeader().equals("null") && !c2.getHeader().isEmpty()) {
	    					//TODO don't add headers that appear multiple times in this table!
	    					disjoint.add(c2.getHeader());
	    				}
	    			}
    			}
    		}
    	}
//    	stat.print();

    	// create sets of disjoint headers (which cannot have a schema correspondence as they appear in the same table)
    	System.out.println("Disjoint Headers:");
    	for(String header : disjointHeaders.keySet()) {
    		Set<String> disjoint = disjointHeaders.get(header);
    		System.out.println(String.format("\t%s\t%s", header, StringUtils.join(disjoint, ",")));
    	}
    	

    	WebTablesMatchingGraph wtmg = new WebTablesMatchingGraph(web.getTables().values());
    	wtmg.setDisjointHeaders(disjointHeaders);
    	
    	/***********************************************
    	 *********************************************** 
    	 * Re-Designed Matching Steps
    	 ***********************************************
    	 ***********************************************/
    	
    	long start = System.currentTimeMillis();
    	
    	System.out.println(String.format("%d records", web.getRecords().size()));
    	DisjointHeaders dh = new DisjointHeaders(disjointHeaders);
    	
    	// print table ids
    	for(Integer id : Q.sort(web.getTables().keySet())) {
    		System.out.println(String.format("#%d\t%s", id, web.getTables().get(id).getPath()));
    	}
    	
//    	N2NGoldStandard instanceGS = new N2NGoldStandard();
//    	File instanceGsFile = new File(evaluationLocation, "instance_gs.tsv");
//    	if(instanceGsFile.exists()) {
//    		instanceGS.loadFromTSV(instanceGsFile);
//    	}
//    	
//    	N2NGoldStandard schemaGS = new N2NGoldStandard();
//    	File schemaGsFile = new File(evaluationLocation, "union_schema_gs.tsv");
////    	File schemaGsFile = new File(evaluationLocation, "goldstandard.tsv");
//    	if(schemaGsFile.exists()) {
//    		schemaGS.loadFromTSV(schemaGsFile);
//    	} 
    	
    	N2NGoldStandard gs = new N2NGoldStandard();
    	File gsFile = new File(evaluationLocation, "goldstandard.tsv");
    	if(gsFile.exists()) {
	    	gs.loadFromTSV(gsFile);
    	}
    	
    	/*********************************************** 
    	 * Initial record links
    	 ***********************************************/
    	
    	Map<String, Performance> instancePerformance = null;
    	N2NGoldStandard instanceMapping = null;
    	Map<String, Performance> schemaPerformance = null;
    	N2NGoldStandard schemaMapping = null;
    	
ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = null;
    	
    	ValueBasedSchemaBlocker valueBasedSchemaBlocker = new ValueBasedSchemaBlocker(false,0.0);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = valueBasedSchemaBlocker.runBlocking(web.getRecords(), true, null, proc);  

    	/*********************************************** 
    	 * match keys
    	 ***********************************************/
    	DeterminantMatcher determinantMatcher = new DeterminantMatcher();
		ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = determinantMatcher.match(web, schemaCorrespondences, proc);

    	/*********************************************** 
    	 * create new record links based on matching keys
    	 ***********************************************/
    	
    	MatchingKeyRecordBlockerImproved2 matchingKeyBlocker = new MatchingKeyRecordBlockerImproved2();
    	keyInstanceCorrespondences = matchingKeyBlocker.runBlocking(web.getRecords(), true, keyCorrespondences, proc);

    	/*********************************************** 
    	 * vote for schema correspondences
    	 ***********************************************/
    	
    	InstanceByKeyToSchemaAggregator instanceVoteForSchema = new InstanceByKeyToSchemaAggregator(2, false, false, false, 1.0);
    	schemaCorrespondences  = instanceVoteForSchema.aggregate(keyInstanceCorrespondences, proc);
        	
		DisjointHeaderSchemaMatchingRule disjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
		schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
    	
    	/*********************************************** 
    	 * refine schema correspondences
    	 ***********************************************/		
    	// add schema correspondences via attribute names
    	SchemaSynonymBlocker generateSynonyms = new SchemaSynonymBlocker();
    	ResultSet<Set<String>> synonyms = generateSynonyms.runBlocking(web.getSchema(), true, schemaCorrespondences, proc);
    	
    	dh.extendWithSynonyms(synonyms.get());
    	
    	SynonymBasedSchemaBlocker synonymBlocker = new SynonymBasedSchemaBlocker(synonyms);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondencesFromSynonyms = synonymBlocker.runBlocking(web.getSchema(), true, null, proc);
    	
    	schemaCorrespondencesFromSynonyms.deduplicate();
    	schemaCorrespondencesFromSynonyms = disjointHeaderRule.run(schemaCorrespondencesFromSynonyms, proc);
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondencesFromSynonyms.get()) {
    		schemaCorrespondences.add(cor);
    	}
    	schemaCorrespondences.deduplicate();
    	
    	// remove special columns from schema correspondences (synonymBlocker is a blocker and uses all columns, so new correspondences for special columns can be created) 
    	SpecialColumnsSchemaFilter specialColumnsFilter = new SpecialColumnsSchemaFilter();
    	schemaCorrespondences = specialColumnsFilter.run(schemaCorrespondences, proc);
    	
    	// refine schema correspondences by applying disjoint header rule
    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
    	
    	/*********************************************** 
    	 * graph-based optimisation
    	 ***********************************************/
    	ResolveConflictsByEdgeBetweenness rb = new ResolveConflictsByEdgeBetweenness(false);
		schemaCorrespondences = rb.match(schemaCorrespondences, proc, dh);
    	
//    	
////    	/*********************************************** 
////    	 * the value-based approach produces all possible, correct links if all tables have the same set of attributes
////    	 * missing correspondences are attributes with no overlapping values/headers between the different tables
////    	 ***********************************************/
////    	ValueBasedSchemaBlocker valueBasedSchemaBlocker = new ValueBasedSchemaBlocker();
////    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> valueBasedSchema = valueBasedSchemaBlocker.runBlocking(web.getRecords(), true, null, proc);
////    	schemaMapping = createMappingForOriginalColumns(valueBasedSchema.get());
////    	schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
////    	System.out.println(gs.formatEvaluationResult(schemaPerformance, true));
////    	System.out.println("v2_schema_graph_value_based.net");
////    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_value_based.net"), web.getSchema(), valueBasedSchema);
////    	
////    	SchemaSynonymBlocker generateSynonyms0 = new SchemaSynonymBlocker();
////    	ResultSet<Set<String>> synonyms0 = generateSynonyms0.runBlocking(web.getSchema(), true, valueBasedSchema, proc);
////    	System.out.println("Synonyms");
////    	for(Set<String> clu : synonyms0.get()) {
////    		System.out.println("\t" + StringUtils.join(clu, ","));
////    	}
////    	dh.extendWithSynonyms(synonyms0.get());
////    	SynonymBasedSchemaBlocker synonymBlocker0 = new SynonymBasedSchemaBlocker(synonyms0);
////    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> valueSchemaCorrespondencesFromSynonyms = synonymBlocker0.runBlocking(web.getSchema(), true, null, proc);    	
////    	valueSchemaCorrespondencesFromSynonyms.deduplicate();
////    	System.out.println(String.format("Synonyms: %d schema correspondences", valueSchemaCorrespondencesFromSynonyms.size()));
////    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : valueSchemaCorrespondencesFromSynonyms.get()) {
////    		valueBasedSchema.add(cor);
////    	}
////    	valueBasedSchema.deduplicate();
////    	System.out.println(String.format("== Total: %d schema correspondences", valueBasedSchema.size()));
////    	
////    	// remove special columns from schema correspondences (synonymBlocker is a blocker and uses all columns, so new correspondences for special columns can be created) 
////    	SpecialColumnsSchemaFilter specialColumnsFilter0 = new SpecialColumnsSchemaFilter();
////    	valueBasedSchema = specialColumnsFilter0.run(valueBasedSchema, proc);
////    	System.out.println(String.format("-> Special Columns: %d schema correspondences", valueBasedSchema.size()));
////    	
////    	schemaMapping = createMappingForOriginalColumns(valueBasedSchema.get());
////    	schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
////    	System.out.println(gs.formatEvaluationResult(schemaPerformance, true));
////    	System.out.println("v2_schema_graph_value_based_synonyms.nett");
////    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_value_based_synonyms.net"), web.getSchema(), valueBasedSchema);
//    	
//    	
////    	
////    	
////       	/*********************************************** 
////    	 * match keys
////    	 ***********************************************/
////
////    	System.out.println(String.format("UCC: %d candidate keys", web.getCandidateKeys().size()));
////    	
////    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> valueKeyCorrespondences = null;
////    	BasicCollection<MatchableTableKey> valueConsolidatedKeys = web.getCandidateKeys();
////    	
////    	// run a blocker with schema correspondences that propagates the keys
////    	KeyBySchemaCorrespondenceBlocker<MatchableTableRow> valueKeyBlocker = new KeyBySchemaCorrespondenceBlocker<>();
////    	valueKeyCorrespondences = valueKeyBlocker.runBlocking(valueConsolidatedKeys, true, valueBasedSchema, proc);
////    	// then consolidate the created keys, i.e., create a dataset with the new keys
////    	CandidateKeyConsolidator<MatchableTableColumn> valueKeyConsolidator = new CandidateKeyConsolidator<>();
////    	valueConsolidatedKeys = valueKeyConsolidator.run(web.getCandidateKeys(), valueKeyCorrespondences, proc);
////    	System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", 1, valueConsolidatedKeys.size(), valueKeyCorrespondences.size()));
////    	printKeys(Q.without(valueConsolidatedKeys.get(), web.getCandidateKeys().get()));
////    
//////    	int vLast = valueConsolidatedKeys.size();
//////    	int vRound = 2;
//////    	do {
//////    		vLast = valueConsolidatedKeys.size();
//////    		valueConsolidatedKeys = valueKeyConsolidator.run(valueConsolidatedKeys, valueKeyCorrespondences, proc);
//////    		valueKeyCorrespondences = valueKeyBlocker.runBlocking(valueConsolidatedKeys, true, valueBasedSchema, proc);
//////    		System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", vRound++, valueConsolidatedKeys.size(), valueKeyCorrespondences.size()));
//////    	} while(valueConsolidatedKeys.size()!=vLast);
////
////    	
////    	writeKeyGraph(valueKeyCorrespondences.get(), new File(resultsLocationFile, "v2_key_graph_2.net"));
////
////    	
////    	/*********************************************** 
////    	 * create new record links based on matching keys
////    	 ***********************************************/
////    	
////    	// match records based on matching keys
////    	MatchingKeyRecordBlocker valueMatchingKeyBlocker = new MatchingKeyRecordBlocker();
////    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> valueKeyInstanceCorrespondences = valueMatchingKeyBlocker.runBlocking(web.getRecords(), true, valueKeyCorrespondences, proc);
////    	writeKeyInstanceGraph(valueKeyInstanceCorrespondences.get(), new File(resultsLocationFile, "v2_instance_graph_1.net"));
////    	
////    	
////    	/*********************************************** 
////    	 * vote for schema correspondences
////    	 ***********************************************/
////    	
////    	// aggregates the votes for schema correspondences and creates the final schema correspondences
////    	InstanceByKeyToSchemaAggregator valueInstanceVoteForSchema = new InstanceByKeyToSchemaAggregator();
////    	valueBasedSchema  = valueInstanceVoteForSchema.aggregate(valueKeyInstanceCorrespondences, proc);
////    	System.out.println(String.format("Voting: %d schema correspondences", valueBasedSchema.size()));
////    	
////    	// remove special columns from schema correspondences
////    	SpecialColumnsSchemaFilter valueSpecialColumnsFilter = new SpecialColumnsSchemaFilter();
////    	valueBasedSchema = valueSpecialColumnsFilter.run(valueBasedSchema, proc);
////    	System.out.println(String.format("-> Special Columns: %d schema correspondences", valueBasedSchema.size()));
////    	
////    	// refine schema correspondences by applying disjoint header rule
////    	DisjointHeaderSchemaMatchingRule valueDisjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
////    	valueBasedSchema = valueDisjointHeaderRule.run(valueBasedSchema, proc);
////    	System.out.println(String.format("-> Disjoint Headers: %d schema correspondences", valueBasedSchema.size()));
////    	
////    	
////    	schemaMapping = N2NGoldStandard.createFromCorrespondences(valueBasedSchema.get());
////    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
////    	System.out.println("v2_schema_graph_2.net");
////    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance, false));
////    	
////    	
////    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_2.net"), web.getSchema(), valueBasedSchema);
////    	
//    	
//    	
//    	
//    	
//    	
//    	
//    	/*********************************************** 
//    	 * the duplicate-based approach would also produce all possible, correct links if all tables have the same set of attributes
//    	 * however, the runtime of testing all attribute combinations is much too high
//    	 * so, this implementation is an approximation and has hence a lower recall
//    	 ***********************************************/
//    	
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = null;
//    	keyInstanceCorrespondences = createRecordLinks();
//    	writeKeyInstanceGraph(keyInstanceCorrespondences.get(), new File(resultsLocationFile, "v2_instance_graph_0.net"));
//
////    	printKeyInstanceCorrespondences(keyInstanceCorrespondences, 7, 33);
////    	printKeyInstanceCorrespondences(keyInstanceCorrespondences, 7, 9);
////    	printKeyInstanceCorrespondences(keyInstanceCorrespondences, 9, 33);
////    	printKeyInstanceCorrespondences(keyInstanceCorrespondences, 4, 27);
//    	
//    	TrustedKeyAggregator trust = new TrustedKeyAggregator(1);
//    	keyInstanceCorrespondences = trust.aggregate(keyInstanceCorrespondences, proc);
//    	
//    	
//    	/*********************************************** 
//    	 * vote for schema correspondences
//    	 ***********************************************/
//    	
//    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = voteForSchema(keyInstanceCorrespondences, dh, gs);
//    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_0.net"), web.getSchema(), schemaCorrespondences);
//
//    	
////    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = valueBasedSchema;
//    	printSchemaSynonyms(schemaCorrespondences);
//    	//TODO maybe use global schema voting here? 
//    	// create all transitive correspondences and re-run a one-to-one mapping step
//
//    	
//    	/*********************************************** 
//    	 * match keys
//    	 ***********************************************/
//
//    	//TODO change this to something similar to trusted keys to get back the high precision: use multiple, maximal keys (those that still produce records links)
//    	// -- loop over key matching & record link creation
//    	// -- keep only those record links from the last iteration, if there are none for their tables in the current iteration (then they're maximal)
//    	// -- replace all keys by larger keys in each iteration (if they produced record links)
//    	// => might allow us to match attributes with sim 1.0 which are dependent on a larger number of attributes
//    	// => the more attributes match, the more certain we can be
//    	// if we additionally loop over schema matching, we can add the matched attributes to the key to avoid checking unmachting combinations
//    	
//    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = matchKeys(schemaCorrespondences);
//    	writeKeyGraph(keyCorrespondences.get(), new File(resultsLocationFile, "v2_key_graph_2.net"));
//
//    	/*********************************************** 
//    	 * create new record links based on matching keys
//    	 ***********************************************/
//    	
//    	System.out.println("Matching records based on candidate keys");
//    	// match records based on matching keys
//    	MatchingKeyRecordBlocker matchingKeyBlocker = new MatchingKeyRecordBlocker();
//    	keyInstanceCorrespondences = matchingKeyBlocker.runBlocking(web.getRecords(), true, keyCorrespondences, proc);
////    	KeyCorrespondenceBasedBlocker keyCorBlocker = new KeyCorrespondenceBasedBlocker();
////    	keyInstanceCorrespondences = keyCorBlocker.runBlocking(web.getRecords(), true, keyCorrespondences, proc);
//    	writeKeyInstanceGraph(keyInstanceCorrespondences.get(), new File(resultsLocationFile, "v2_instance_graph_1.net"));
//    	
//
////    	printKeyInstanceCorrespondences(keyInstanceCorrespondences, 28, 30);
//    	
//    	/*********************************************** 
//    	 * vote for schema correspondences
//    	 ***********************************************/
//    	
//    	schemaCorrespondences = voteForSchema(keyInstanceCorrespondences, dh, gs);
//    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_2.net"), web.getSchema(), schemaCorrespondences);
//    	
//    	
//    	/*********************************************** 
//    	 * refine schema correspondences
//    	 ***********************************************/
//    	
//    	schemaCorrespondences = refineSchemaCorrespondences(schemaCorrespondences, dh, gs);
//    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_3.net"), web.getSchema(), schemaCorrespondences);
    	
    	long end = System.currentTimeMillis();
    	
    	System.out.println(String.format("Matching finished after %s", DurationFormatUtils.formatDurationHMS(end-start)));
    	
    	start = System.currentTimeMillis();
    	
    	// merge the tables
    	TableReconstructor tr = new TableReconstructor();
    	Table reconstructed = Q.firstOrDefault(tr.reconstruct(Q.max(web.getTables().keySet()), web.getRecords(), web.getSchema(), web.getCandidateKeys(), schemaCorrespondences));
    	CSVTableWriter csvw = new CSVTableWriter();
    	csvw.write(reconstructed, new File(resultsLocationFile, reconstructed.getPath()));
    	
    	end = System.currentTimeMillis();
    	
    	System.out.println(String.format("Reconstruction finished after %s", DurationFormatUtils.formatDurationHMS(end-start)));
    	
    	System.out.println("done");
    	

    	
	}

	public N2NGoldStandard createMappingForOriginalColumns(Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences) {
		HashSet<MatchableTableColumn> nodes = new HashSet<>();
		ConnectedComponentClusterer<MatchableTableColumn> comp = new ConnectedComponentClusterer<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : correspondences) {
			if(cor.getSimilarityScore()>0.0) {
				nodes.add(cor.getFirstRecord());
				nodes.add(cor.getSecondRecord());
				
				comp.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
			}
		}
		
		for(MatchableTableColumn node : nodes) {
			comp.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(node, node, 1.0));
		}
		
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clusters = comp.createResult();
		N2NGoldStandard n2n = new N2NGoldStandard();
		
		for(Collection<MatchableTableColumn> cluster : clusters.keySet()) {
			HashSet<String> originalColumns = new HashSet<>();
			
			for(MatchableTableColumn c : cluster) {
				if(!SpecialColumns.isSpecialColumn(c) && !ContextColumns.isContextColumn(c)) {
					TableColumn col = web.getTables().get(c.getTableId()).getSchema().get(c.getColumnIndex());
					originalColumns.addAll(col.getProvenance());
				}
			}
			
			n2n.getCorrespondenceClusters().put(originalColumns, Q.firstOrDefault(originalColumns));
		}
		
		return n2n;
	}
	
	public Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> matches(Collection<Integer> candKey, MatchableTableRow record1, MatchableTableRow record2) {
		
		// get the key values for candKey in table1
		LinkedList<String> t1Values = new LinkedList<>();
		for(Integer index : candKey) {
			Object value = record1.get(index);
			String stringValue = null;
			if(value==null) {
				stringValue = "NULL";
			} else {
				stringValue = value.toString();
			}
			t1Values.add(stringValue);
		}
		
		// try to find the values in table2
		Collection<Integer> matchingValueIndices = new HashSet<>();
		MatchableTableRow row2 = record2;
		for(int i = 0; i < row2.getRowLength(); i++) {
			Object value = row2.get(i);
			String stringValue = null;
			if(value==null) {
				stringValue = "NULL";
			} else {
				stringValue = value.toString();
			}
			
			if(t1Values.contains(stringValue)) {
				matchingValueIndices.add(i);
				t1Values.remove(stringValue);
			}
		}
		
		// have all values been found?
		if(t1Values.size()==0) {
			// if yes, create a new correspondence
			Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> newCor = new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(
					new MatchableTableRowWithKey(record1, candKey), 
					new MatchableTableRowWithKey(record2, matchingValueIndices));
			return newCor;
		} else {
			return null;
		}
	}

	public void printSchemaCorrespondences(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences) {
		
		Map<String, Collection<Correspondence<MatchableTableColumn, MatchableTableRow>>> grouped = Q.group(correspondences.get(), new Func<String, Correspondence<MatchableTableColumn, MatchableTableRow>>() {

			@Override
			public String invoke(Correspondence<MatchableTableColumn, MatchableTableRow> in) {
				return "#" + in.getFirstRecord().getTableId() + "<-> #" + in.getSecondRecord().getTableId();
			}
		});
		
		for(String group : grouped.keySet()) {
			
			System.out.println(group);
			
			SimilarityMatrix<MatchableTableColumn> m = SimilarityMatrix.fromCorrespondences(grouped.get(group), new SparseSimilarityMatrixFactory());
			System.out.println(m.getOutput());
//			for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : grouped.get(group)) {
//				System.out.println(String.format("\t%s<->%s", cor.getFirstRecord(), cor.getSecondRecord()));
//			}
			
		}
	}

	public void printKeyInstanceCorrespondences(ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences, int id1, int id2) {
		RecordKeyValueMapper<String, Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>> groupByTableCombination = new RecordKeyValueMapper<String, Correspondence<MatchableTableRow,MatchableTableKey>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
		
			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableKey> record,
					DatasetIterator<Pair<String, Correspondence<MatchableTableRow, MatchableTableKey>>> resultCollector) {
				
				String key = record.getFirstRecord().getTableId() + "/" + record.getSecondRecord().getTableId();
				
				resultCollector.next(new Pair<String, Correspondence<MatchableTableRow,MatchableTableKey>>(key, record));
				
			}
		};
		
		
		ResultSet<Group<String, Correspondence<MatchableTableRow, MatchableTableKey>>> byTable = proc.groupRecords(correspondences, groupByTableCombination);
		
		for(Group<String, Correspondence<MatchableTableRow, MatchableTableKey>> group : byTable.get()) {
			
			Correspondence<MatchableTableRow, MatchableTableKey> firstCor = Q.firstOrDefault(group.getRecords().get());
			
			int t1 = firstCor.getFirstRecord().getTableId();
			int t2 = firstCor.getSecondRecord().getTableId();
			
			if( (id1==-1||t1==id1) && (id2==-1||t2==id2)) {
			
				System.out.println(String.format("Tables %d<->%d", t1, t2));
				for(Correspondence<MatchableTableRow, MatchableTableKey> cor : group.getRecords().get()) {
					System.out.println(String.format("\t[#%d]\t%s", cor.getFirstRecord().getRowNumber(), cor.getFirstRecord().format(20)));
					System.out.println(String.format("\t[#%d]\t%s", cor.getSecondRecord().getRowNumber(), cor.getSecondRecord().format(20)));
				}
			
			}
		}
	}
	
	public void printInstanceCorrespondences(ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences, int id1, int id2) {
		RecordKeyValueMapper<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>> groupByTableCombination = new RecordKeyValueMapper<String, Correspondence<MatchableTableRow,MatchableTableColumn>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
		
			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableColumn> record,
					DatasetIterator<Pair<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {
				
				String key = record.getFirstRecord().getTableId() + "/" + record.getSecondRecord().getTableId();
				
				resultCollector.next(new Pair<String, Correspondence<MatchableTableRow,MatchableTableColumn>>(key, record));
				
			}
		};
		
		
		ResultSet<Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> byTable = proc.groupRecords(correspondences, groupByTableCombination);
		
		for(Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>> group : byTable.get()) {
			
			Correspondence<MatchableTableRow, MatchableTableColumn> firstCor = Q.firstOrDefault(group.getRecords().get());
			
			int t1 = firstCor.getFirstRecord().getTableId();
			int t2 = firstCor.getSecondRecord().getTableId();
			
			if( (id1==-1||t1==id1) && (id2==-1||t2==id2)) {
			
				System.out.println(String.format("Tables %d<->%d", t1, t2));
				for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : group.getRecords().get()) {
					System.out.println(String.format("\t[#%d]\t%s", cor.getFirstRecord().getRowNumber(), cor.getFirstRecord().format(20)));
					System.out.println(String.format("\t[#%d]\t%s", cor.getSecondRecord().getRowNumber(), cor.getSecondRecord().format(20)));
				}
			
			}
		}
	}
	
	private void printKeyCorrespondences(Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences) {
		
		List<Correspondence<MatchableTableKey, MatchableTableColumn>> sorted = Q.sort(keyCorrespondences, new Comparator<Correspondence<MatchableTableKey, MatchableTableColumn>>() {

			@Override
			public int compare(Correspondence<MatchableTableKey, MatchableTableColumn> o1,
					Correspondence<MatchableTableKey, MatchableTableColumn> o2) {
				int result = Integer.compare(o1.getFirstRecord().getTableId(), o2.getFirstRecord().getTableId());
				
				if(result!=0) {
					return result;
				}
				
				result = Integer.compare(o1.getSecondRecord().getTableId(), o2.getSecondRecord().getTableId());
				
				if(result!=0) {
					return result;
				}
				
				return Integer.compare(o1.getFirstRecord().getColumns().size(), o2.getFirstRecord().getColumns().size());
			}
		});
		
		for(Correspondence<MatchableTableKey, MatchableTableColumn> key : sorted) {
			System.out.println(String.format("{#%d}{%s} <-> {#%d}{%s}", 
					key.getFirstRecord().getTableId(), 
					Q.project(key.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()),
					key.getSecondRecord().getTableId(), 
					Q.project(key.getSecondRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection())));
		}
		
	}
	
	public void printVotesForKeys(ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences,
			DataProcessingEngine proc, final int id1, final int id2) {
		
		RecordKeyValueMapper<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>> groupByTablesMapper = new RecordKeyValueMapper<Collection<Integer>, Correspondence<MatchableTableRow,MatchableTableKey>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableKey> record,
					DatasetIterator<Pair<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>>> resultCollector) {
				
				if(record.getFirstRecord().getTableId()>record.getSecondRecord().getTableId()) {
					System.out.println("Wrong direction!");
				}
				
				if( (id1==-1||record.getFirstRecord().getTableId()==id1) && (id2==-1||record.getSecondRecord().getTableId()==id2)) {
					resultCollector.next(new Pair<Collection<Integer>, Correspondence<MatchableTableRow,MatchableTableKey>>(Q.toList(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				}
			}
		};
		
		// group by table
		ResultSet<Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>>> grouped = proc.groupRecords(correspondences, groupByTablesMapper);
		
		RecordMapper<Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableRow, MatchableTableKey>> removeUntrustedCorrespondences = new RecordMapper<Group<Collection<Integer>,Correspondence<MatchableTableRow,MatchableTableKey>>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableKey>> resultCollector) {

				System.out.println(String.format("*** %s ***", record.getKey()));
				
				// combine all correspondences between the same records (with different keys)
				HashMap<List<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>> correspondences = new HashMap<>();
				
				// count the key combinations in the correspondences
				SimilarityMatrix<MatchableTableKey> keyFrequencies = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
				for(Correspondence<MatchableTableRow, MatchableTableKey> cor : record.getRecords().get()) {
					List<Integer> rows = Q.toList(cor.getFirstRecord().getRowNumber(), cor.getSecondRecord().getRowNumber());
					Correspondence<MatchableTableRow, MatchableTableKey> merged = correspondences.get(rows);
					if(merged==null) {
						merged = new Correspondence<MatchableTableRow, MatchableTableKey>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), new ResultSet<Correspondence<MatchableTableKey, MatchableTableRow>>());
						correspondences.put(rows, merged);
					}
					
					for(Correspondence<MatchableTableKey, MatchableTableRow> keyCor : cor.getCausalCorrespondences().get()) {
					
						merged.getCausalCorrespondences().add(keyCor);
						
						MatchableTableKey k1 = keyCor.getFirstRecord();
						MatchableTableKey k2 = keyCor.getSecondRecord();
						
						Double existing = keyFrequencies.get(k1, k2);
						if(existing==null) {
							existing=0.0;
						}
						
						keyFrequencies.set(k1, k2, existing+1.0);
					}
				}
				
				System.out.println(keyFrequencies.getOutput());
				
				
				// decide for the most frequent combinations
				BestChoiceMatching bcm = new BestChoiceMatching();
				SimilarityMatrix<MatchableTableKey> trustedKeys = bcm.match(keyFrequencies);
				
				Correspondence<MatchableTableRow, MatchableTableKey> anyCor = Q.firstOrDefault(correspondences.values());
				System.out.println(String.format("Table #%d: %d candidate keys", anyCor.getFirstRecord().getTableId(), anyCor.getFirstRecord().getKeys().length));
				for(MatchableTableColumn[] key : anyCor.getFirstRecord().getKeys()) {
					System.out.println(String.format("\t%s", Arrays.asList(key)));
				}
				System.out.println(String.format("Table #%d: %d candidate keys", anyCor.getSecondRecord().getTableId(), anyCor.getSecondRecord().getKeys().length));
				for(MatchableTableColumn[] key : anyCor.getSecondRecord().getKeys()) {
					System.out.println(String.format("\t%s", Arrays.asList(key)));
				}
				
				trustedKeys.normalize();
				trustedKeys.prune(1.0);
				
				System.out.println(trustedKeys.getOutput());
				
				for(MatchableTableKey k1 : trustedKeys.getFirstDimension()) {
					for(MatchableTableKey k2 : trustedKeys.getMatches(k1)) {
						System.out.println(String.format("%s<->%s", k1, k2));
					}
				}
				
				// only output correspondences where all the frequent combinations matched
				for(Correspondence<MatchableTableRow, MatchableTableKey> cor : correspondences.values()) {
					
					// check all key correspondences
					Iterator<Correspondence<MatchableTableKey, MatchableTableRow>> keyIt = cor.getCausalCorrespondences().get().iterator();
					while(keyIt.hasNext()) {
						Correspondence<MatchableTableKey, MatchableTableRow> keyCor = keyIt.next();
						
						if(trustedKeys.get(keyCor.getFirstRecord(), keyCor.getSecondRecord())==null) {
							// this is an untrusted key and hence removed
							keyIt.remove();
						}
					}
					
					// now only trusted key correspondences remain
					// if the number of key correspondences does not match the number of trusted keys, the instance correspondence is untrusted and removed
					if(cor.getCausalCorrespondences().size()==trustedKeys.getNumberOfNonZeroElements()) {
						System.out.println(String.format("++ %s", cor.getFirstRecord().format(20)));
						System.out.println(String.format("++ %s", cor.getSecondRecord().format(20)));
					} else {
						
//						System.out.println(keyFrequencies.getOutput());
//						System.out.println(trustedKeys.getOutput());
//						
//						for(MatchableTableKey k1 : trustedKeys.getFirstDimension()) {
//							for(MatchableTableKey k2 : trustedKeys.getMatches(k1)) {
//								
//								boolean found = false;
//								for(Correspondence<MatchableTableKey, MatchableTableRow> keyCor : cor.getCausalCorrespondences().get()) {
//									if(keyCor.getFirstRecord().equals(k1) && keyCor.getSecondRecord().equals(k2)) {
//										found=true;
//										break;
//									}
//								}
//								
//								if(!found) {
//									System.out.println(String.format("-x {%s}<->{%s}", k1.getColumns(), k2.getColumns()));
//								}
//								
//							}
//						}
//						
						System.out.println(String.format("-- %s", cor.getFirstRecord().format(20)));
						System.out.println(String.format("-- %s", cor.getSecondRecord().format(20)));
//						
//						System.out.println();
					}
					
				}
			}
		};
		
		proc.transform(grouped, removeUntrustedCorrespondences);
		
	}

	public void printInstanceMappingDetails(N2NGoldStandard mapping) {
		
		for(Set<String> cluster : mapping.getCorrespondenceClusters().keySet()) {
			
			System.out.println(mapping.getCorrespondenceClusters().get(cluster));
			for(String record : cluster) {
				MatchableTableRow row = web.getRecords().getRecord(record);
				System.out.println(String.format("%s\t%s", row.getIdentifier(), row.format(30)));
			}
			
		}
		
	}
	
	public <T2> N2NGoldStandard createSchemaMappingFromCorrespondences(Collection<Correspondence<MatchableTableColumn, T2>> correspondences) {
		ConnectedComponentClusterer<TableColumn> comp = new ConnectedComponentClusterer<>();
		
		for(Table t : web.getTables().values()) {
			for(TableColumn c : t.getColumns()) {
				if(!SpecialColumns.isSpecialColumn(c)) {
					comp.addEdge(new Triple<TableColumn, TableColumn, Double>(c, c, 1.0));
				}
			}
		}
		
		for(Correspondence<MatchableTableColumn, T2> cor : correspondences) {
		
			MatchableTableColumn mc1 = cor.getFirstRecord();
			TableColumn col1 = web.getTables().get(mc1.getTableId()).getSchema().getRecord(mc1.getIdentifier());
			
			MatchableTableColumn mc2 = cor.getSecondRecord();
			TableColumn col2 = web.getTables().get(mc2.getTableId()).getSchema().getRecord(mc2.getIdentifier());

			comp.addEdge(new Triple<TableColumn, TableColumn, Double>(col1, col2, cor.getSimilarityScore()));
		}
		
		Map<Collection<TableColumn>, TableColumn> clusters = comp.createResult();
		N2NGoldStandard n2n = new N2NGoldStandard();
		
		for(Collection<TableColumn> cluster : clusters.keySet()) {
			Set<String> columns = new HashSet<>();
			
			for(TableColumn c : cluster) {
				columns.addAll(c.getProvenance());
			}
			
			TableColumn first = Q.firstOrDefault(cluster);
			String name = String.format("[%d]%s", first.getColumnIndex(), first.getHeader());
			
			n2n.getCorrespondenceClusters().put(columns, name);
		}
		
		return n2n;
	}
	
	public void printVotesForSchemaCorrespondences(Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, final int id1, final int id2) {
		
		Map<String, Collection<Correspondence<MatchableTableRow, MatchableTableColumn>>> grouped = Q.group(instanceCorrespondences, new Func<String, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

			@Override
			public String invoke(Correspondence<MatchableTableRow, MatchableTableColumn> in) {
				if ( (id1==-1||id1==in.getFirstRecord().getTableId()) && (id2==-1||id2==in.getSecondRecord().getTableId())) {
					return "#" + in.getFirstRecord().getTableId() + "<->" +in.getSecondRecord().getTableId();
				} else {
					return "";
				}
			}});
		
		for(String group : grouped.keySet()) {
			if(!group.equals("")) {
				
				System.out.println(group);
				
				Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> votes = new LinkedList<>();
				for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : grouped.get(group)) {
					System.out.println(String.format("\t%s", cor.getFirstRecord().format(20)));
					System.out.println(String.format("\t%s", cor.getSecondRecord().format(20)));
					votes.addAll(cor.getCausalCorrespondences().get());
				}
				
				SimilarityMatrix<MatchableTableColumn> m = SimilarityMatrix.fromCorrespondences(votes, new SparseSimilarityMatrixFactory());
				
				System.out.println(m.getOutput());
				
			}
		}
	}
	
	public void writeKeyInstanceGraph(Collection<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences, File file) throws IOException {
		
		Map<Set<Integer>, Collection<Correspondence<MatchableTableRow, MatchableTableKey>>> grouped = Q.group(correspondences, new Func<Set<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>>() {

			@Override
			public Set<Integer> invoke(Correspondence<MatchableTableRow, MatchableTableKey> in) {
				return Q.toSet(in.getFirstRecord().getTableId(), in.getSecondRecord().getTableId());
			}});
		
		WebTablesMatchingGraph graph = new WebTablesMatchingGraph(web.getTables().values());
		
		for(Set<Integer> group : grouped.keySet()) {
			List<Integer> sorted = Q.sort(group);
			Table t1 = web.getTables().get(sorted.get(0));
			Table t2 = web.getTables().get(sorted.get(1));
			
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceMapping = new ResultSet<>();
			
			for(Correspondence<MatchableTableRow, MatchableTableKey> cor : grouped.get(group)) {
				Correspondence<MatchableTableRow, MatchableTableColumn> newCor = new Correspondence<MatchableTableRow, MatchableTableColumn>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), null);
				instanceMapping.add(newCor);
			}

			
			graph.addInstanceCorrespondence(t1, t2, instanceMapping);
		}
		
		graph.writeInstanceCorrespondenceGraph(file);
	}
	
	public void writeInstanceGraph(Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences, File file) throws IOException {
		
		Map<Set<Integer>, Collection<Correspondence<MatchableTableRow, MatchableTableColumn>>> grouped = Q.group(correspondences, new Func<Set<Integer>, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

			@Override
			public Set<Integer> invoke(Correspondence<MatchableTableRow, MatchableTableColumn> in) {
				return Q.toSet(in.getFirstRecord().getTableId(), in.getSecondRecord().getTableId());
			}});
		
		WebTablesMatchingGraph graph = new WebTablesMatchingGraph(web.getTables().values());
		
		for(Set<Integer> group : grouped.keySet()) {
			List<Integer> sorted = Q.sort(group);
			Table t1 = web.getTables().get(sorted.get(0));
			Table t2 = web.getTables().get(sorted.get(1));
			
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceMapping = new ResultSet<>(grouped.get(group));
			
			graph.addInstanceCorrespondence(t1, t2, instanceMapping);
		}
		
		graph.writeInstanceCorrespondenceGraph(file);
	}
	
	public void writeKeyGraph(Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> correspondences, File file) throws IOException {
		
		Map<Set<Integer>, Collection<Correspondence<MatchableTableKey, MatchableTableColumn>>> grouped = Q.group(correspondences, new Func<Set<Integer>, Correspondence<MatchableTableKey, MatchableTableColumn>>() {

			@Override
			public Set<Integer> invoke(Correspondence<MatchableTableKey, MatchableTableColumn> in) {
				return Q.toSet(in.getFirstRecord().getTableId(), in.getSecondRecord().getTableId());
			}});
		
		WebTablesMatchingGraph graph = new WebTablesMatchingGraph(web.getTables().values());
		
		for(Set<Integer> group : grouped.keySet()) {
			List<Integer> sorted = Q.sort(group);
			Table t1 = web.getTables().get(sorted.get(0));
			Table t2 = web.getTables().get(sorted.get(1));
			
			Set<WebTableMatchingKey> keys = new HashSet<>();
			
			for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : grouped.get(group)) {
				Set<TableColumn> col1 = new HashSet<>();
				for(MatchableTableColumn mc : cor.getFirstRecord().getColumns()) {
					col1.add(web.getTables().get(mc.getTableId()).getSchema().get(mc.getColumnIndex()));
				}
				Set<TableColumn> col2 = new HashSet<>();
				for(MatchableTableColumn mc : cor.getSecondRecord().getColumns()) {
					col2.add(web.getTables().get(mc.getTableId()).getSchema().get(mc.getColumnIndex()));
				}
				
				WebTableMatchingKey k = new WebTableMatchingKey(col1, col2);
				
				keys.add(k);
			}
			
			graph.addMatchingKeys(t1, t2, keys);
		}
		
		graph.writeMatchingKeyGraph(file);
	}
	
	public void writeSchemaCorrespondenceGraph(File f, DataSet<MatchableTableColumn, MatchableTableColumn> allAttributes, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) throws IOException {
		Graph<MatchableTableColumn, Object> g = new Graph<>();
		
		for(MatchableTableColumn c : allAttributes.get()) {
			if(!SpecialColumns.isSpecialColumn(c)) {
				g.addNode(c);
			}
		}
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			g.addEdge(cor.getFirstRecord(), cor.getSecondRecord(), cor, cor.getSimilarityScore());
		}
	
		g.writePajekFormat(f);
	}
	
	public void printKeys(Collection<MatchableTableKey> keys) {
		Map<Integer, Collection<MatchableTableKey>> grouped = Q.group(keys, new MatchableTableKey.TableIdProjection());
		
		List<Integer> tables = Q.sort(grouped.keySet());
		
		for(Integer tableId : tables) {
			Collection<MatchableTableKey> tableKeys = grouped.get(tableId);
			
			System.out.println(String.format("#%d", tableId));
			
			for(MatchableTableKey k : tableKeys) {
				System.out.println(String.format("\t%s", k.getColumns()));
			}
		}
	}
	

	protected void printSchemaSynonyms(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {	
    	// add schema correspondences via attribute names
    	SchemaSynonymBlocker generateSynonyms = new SchemaSynonymBlocker();
    	ResultSet<Set<String>> synonyms = generateSynonyms.runBlocking(web.getSchema(), true, schemaCorrespondences, proc);
    	System.out.println("Synonyms");
    	for(Set<String> clu : synonyms.get()) {
    		System.out.println("\t" + StringUtils.join(clu, ","));
    	}
	}
	
	protected ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> createRecordLinks() {
    	// creates instance correspondences between the tables by matching the values of candidate keys
    	KeyBasedBlocker blocker = new KeyBasedBlocker();
    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = blocker.runBlocking(web.getRecords(), true, null, proc);
    	// we might have found a record link multiple times, if multiple keys matched, but as we re-check the keys in the next step anyways, we can safely discard them
    	keyInstanceCorrespondences.deduplicate(); 
    	System.out.println(String.format("Blocking: %d instance correspondences", keyInstanceCorrespondences.size()));
//    	printInstanceMappingDetails(instanceMapping);
    	
    	// if the blocker linked two records, check all possible key correspondences and create key matches if possible
    	// - this is already a key propagation: if the key on one side matches a key on the other side plus additional attributes, we would not have found that link in the blocker
    	KeyMatchingRefiner keyRefiner = new KeyMatchingRefiner(1.0);
    	keyInstanceCorrespondences = keyRefiner.run(keyInstanceCorrespondences, proc);
    	System.out.println(String.format("-> Key Refiner: %d instance correspondences", keyInstanceCorrespondences.size()));
    	
    	return keyInstanceCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> voteForSchema(ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences, DisjointHeaders dh, N2NGoldStandard gs) {
    	InstanceByKeyToSchemaAggregator instanceVoteForSchema = new InstanceByKeyToSchemaAggregator(2,false, false, false, 1.0);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences  = instanceVoteForSchema.aggregate(keyInstanceCorrespondences, proc);
    	System.out.println(String.format("Voting: %d schema correspondences", schemaCorrespondences.size()));
    	
    	N2NGoldStandard schemaMapping = createMappingForOriginalColumns(schemaCorrespondences.get());
    	ClusteringPerformance schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
    	System.out.println(gs.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), true));
    	
		DisjointHeaderSchemaMatchingRule disjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
		schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
		
    	return schemaCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> refineSchemaCorrespondences(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, DisjointHeaders dh, N2NGoldStandard gs) {
		DisjointHeaderSchemaMatchingRule disjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
		
    	// add schema correspondences via attribute names
    	SchemaSynonymBlocker generateSynonyms = new SchemaSynonymBlocker();
    	ResultSet<Set<String>> synonyms = generateSynonyms.runBlocking(web.getSchema(), true, schemaCorrespondences, proc);
    	System.out.println("Synonyms");
    	for(Set<String> clu : synonyms.get()) {
    		System.out.println("\t" + StringUtils.join(clu, ","));
    	}
    	dh.extendWithSynonyms(synonyms.get());
    	SynonymBasedSchemaBlocker synonymBlocker = new SynonymBasedSchemaBlocker(synonyms);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondencesFromSynonyms = synonymBlocker.runBlocking(web.getSchema(), true, null, proc);    	
    	schemaCorrespondencesFromSynonyms.deduplicate();
    	System.out.println(String.format("Synonyms: %d schema correspondences", schemaCorrespondencesFromSynonyms.size()));
    	schemaCorrespondencesFromSynonyms = disjointHeaderRule.run(schemaCorrespondencesFromSynonyms, proc);
    	System.out.println(String.format("--> Disjoint Headers - - Synonyms: %d schema correspondences", schemaCorrespondencesFromSynonyms.size()));
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondencesFromSynonyms.get()) {
    		schemaCorrespondences.add(cor);
    	}
    	schemaCorrespondences.deduplicate();
    	System.out.println(String.format("== Total: %d schema correspondences", schemaCorrespondences.size()));
    	
    	N2NGoldStandard schemaMapping = createMappingForOriginalColumns(schemaCorrespondences.get());
    	ClusteringPerformance schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
    	System.out.println(gs.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), true));
    	
    	// remove special columns from schema correspondences (synonymBlocker is a blocker and uses all columns, so new correspondences for special columns can be created) 
    	SpecialColumnsSchemaFilter specialColumnsFilter = new SpecialColumnsSchemaFilter();
    	schemaCorrespondences = specialColumnsFilter.run(schemaCorrespondences, proc);
    	System.out.println(String.format("-> Special Columns: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// refine schema correspondences by applying disjoint header rule
    	
    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
    	System.out.println(String.format("-> Disjoint Headers: %d schema correspondences", schemaCorrespondences.size()));
    	
    	schemaMapping = createMappingForOriginalColumns(schemaCorrespondences.get());
    	schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
    	System.out.println(gs.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), true));
    	
    	schemaMapping = createMappingForOriginalColumns(schemaCorrespondences.get());
    	schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	System.out.println("v2_schema_graph_1.net");
    	System.out.println(gs.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), false));
    	
    	ClusteringPerformance schemaPerformanceInv = gs.evaluateCorrespondenceClustersInverse(schemaMapping.getCorrespondenceClusters().keySet(), false);
    	System.out.println("Inverted Evaluation");
    	System.out.println(gs.formatEvaluationResult(schemaPerformanceInv.getPerformanceByCluster(), true));
    	
    	return schemaCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> matchKeys(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = null;
    	
    	System.out.println(String.format("UCC: %d candidate keys", web.getCandidateKeys().size()));
    	
    	
    	BasicCollection<MatchableTableKey> consolidatedKeys = web.getCandidateKeys();
    	
    	// run a blocker with schema correspondences that propagates the keys
    	KeyBySchemaCorrespondenceBlocker<MatchableTableRow> keyBlocker = new KeyBySchemaCorrespondenceBlocker<>(false);
    	keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    	// then consolidate the created keys, i.e., create a dataset with the new keys
    	CandidateKeyConsolidator<MatchableTableColumn> keyConsolidator = new CandidateKeyConsolidator<>();
    	consolidatedKeys = keyConsolidator.run(web.getCandidateKeys(), keyCorrespondences, proc);
    	System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", 1, consolidatedKeys.size(), keyCorrespondences.size()));
//    	printKeys(Q.without(consolidatedKeys.get(), web.getCandidateKeys().get()));
    	
    	Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> keysBefore = null;
    	Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> keysAfter = keyCorrespondences.get();
    	Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> diff = null; 
    	
    	int last = consolidatedKeys.size();
    	int round = 2;
    	do {
    		keysBefore = keysAfter;
    		last = consolidatedKeys.size();
    		keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    		consolidatedKeys = keyConsolidator.run(consolidatedKeys, keyCorrespondences, proc);
    		System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", round++, consolidatedKeys.size(), keyCorrespondences.size()));
    		keysAfter = keyCorrespondences.get();
//    		diff = Q.without(keysAfter, keysBefore);
//    		printKeyCorrespondences(diff);
    	} while(consolidatedKeys.size()!=last);
    	
    	return keyCorrespondences;
	}
	
}
