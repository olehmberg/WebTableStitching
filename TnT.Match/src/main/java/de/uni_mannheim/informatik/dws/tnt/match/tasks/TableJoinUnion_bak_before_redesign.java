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
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
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
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.SynonymBasedSchemaBlocker;
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
public class TableJoinUnion_bak_before_redesign extends TnTTask {

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
		TableJoinUnion_bak_before_redesign tju = new TableJoinUnion_bak_before_redesign();

		if (tju.parseCommandLine(TableJoinUnion_bak_before_redesign.class, args)) {

			hello();

			tju.initialise();
//			tju.setDataProcessingEngine(new ParallelDataProcessingEngine());
			tju.setMatchingEngine(new MatchingEngine<MatchableTableRow, MatchableTableColumn>());
//			tju.setMatchingEngine(new ParallelMatchingEngine<MatchableTableRow, MatchableTableColumn>());
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
//    	N2NGoldStandardCreator gsc = new N2NGoldStandardCreator();
//    	gsc.createFromMappedUnionTables(web.getTables().values(), evaluationLocation);
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
    	
    	N2NGoldStandard instanceGS = new N2NGoldStandard();
    	File instanceGsFile = new File(evaluationLocation, "instance_gs.tsv");
    	if(instanceGsFile.exists()) {
    		instanceGS.loadFromTSV(instanceGsFile);
    	}
    	
    	N2NGoldStandard schemaGS = new N2NGoldStandard();
    	File schemaGsFile = new File(evaluationLocation, "union_schema_gs.tsv");
//    	File schemaGsFile = new File(evaluationLocation, "goldstandard.tsv");
    	if(schemaGsFile.exists()) {
    		schemaGS.loadFromTSV(schemaGsFile);
    	} 
    	
    	// schema gs for union w/ alias case
//    	Map<Integer, Set<String>> columnsByIndex = new HashMap<>();
//    	for(Table t : web.getTables().values()) {
//    		for(TableColumn c : t.getColumns()) {
//    			if(!SpecialColumns.isSpecialColumn(c)) {
//    				Set<String> columns = MapUtils.get(columnsByIndex, c.getColumnIndex(), new HashSet<String>());
//    				columns.add(c.getIdentifier());
//    			}
//    		}
//    	}
//    	for(Integer i : columnsByIndex.keySet()) {
//    		schemaGS.getCorrespondenceClusters().put(columnsByIndex.get(i), i.toString());
//    	}
//    	schemaGS.writeToTSV(schemaGsFile);
    	
    	/*********************************************** 
    	 * Initial record links
    	 ***********************************************/
    	
    	Map<String, Performance> instancePerformance = null;
    	N2NGoldStandard instanceMapping = null;
    	ClusteringPerformance schemaPerformance = null;
    	N2NGoldStandard schemaMapping = null;
    	
    	
    	
    	
    	
    	
    	/*********************************************** 
    	 * the value-based approach produces all possible, correct links if all tables have the same set of attributes
    	 * missing correspondences are attributes with no overlapping values/headers between the different tables
    	 ***********************************************/
//    	ValueBasedSchemaBlocker valueBasedSchemaBlocker = new ValueBasedSchemaBlocker();
//    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> valueBasedSchema = valueBasedSchemaBlocker.runBlocking(web.getRecords(), true, null, proc);
//    	schemaMapping = N2NGoldStandard.createFromCorrespondences(valueBasedSchema.get());
//    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
//    	System.out.println("v2_schema_graph_value_based.net");
//    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance, false));
//    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_value_based.net"), web.getSchema(), valueBasedSchema);
//    	
//    	SchemaSynonymBlocker generateSynonyms0 = new SchemaSynonymBlocker();
//    	ResultSet<Set<String>> synonyms0 = generateSynonyms0.runBlocking(web.getSchema(), true, valueBasedSchema, proc);
//    	System.out.println("Synonyms");
//    	for(Set<String> clu : synonyms0.get()) {
//    		System.out.println("\t" + StringUtils.join(clu, ","));
//    	}
//    	dh.extendWithSynonyms(synonyms0.get());
//    	SynonymBasedSchemaBlocker synonymBlocker0 = new SynonymBasedSchemaBlocker(synonyms0);
//    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> valueSchemaCorrespondencesFromSynonyms = synonymBlocker0.runBlocking(web.getSchema(), true, null, proc);    	
//    	valueSchemaCorrespondencesFromSynonyms.deduplicate();
//    	System.out.println(String.format("Synonyms: %d schema correspondences", valueSchemaCorrespondencesFromSynonyms.size()));
//    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : valueSchemaCorrespondencesFromSynonyms.get()) {
//    		valueBasedSchema.add(cor);
//    	}
//    	valueBasedSchema.deduplicate();
//    	System.out.println(String.format("== Total: %d schema correspondences", valueBasedSchema.size()));
//    	
//    	// remove special columns from schema correspondences (synonymBlocker is a blocker and uses all columns, so new correspondences for special columns can be created) 
//    	SpecialColumnsSchemaFilter specialColumnsFilter0 = new SpecialColumnsSchemaFilter();
//    	valueBasedSchema = specialColumnsFilter0.run(valueBasedSchema, proc);
//    	System.out.println(String.format("-> Special Columns: %d schema correspondences", valueBasedSchema.size()));
//    	
//    	schemaMapping = N2NGoldStandard.createFromCorrespondences(valueBasedSchema.get());
//    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
//    	System.out.println("v2_schema_graph_value_based_synonyms.nett");
//    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance, false));
//    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_value_based_synonyms.net"), web.getSchema(), valueBasedSchema);
//    	
    	
    	
    	
    	
    	
//    	
//    	
//       	/*********************************************** 
//    	 * match keys
//    	 ***********************************************/
//
//    	System.out.println(String.format("UCC: %d candidate keys", web.getCandidateKeys().size()));
//    	
//    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> valueKeyCorrespondences = null;
//    	BasicCollection<MatchableTableKey> valueConsolidatedKeys = web.getCandidateKeys();
//    	
//    	// run a blocker with schema correspondences that propagates the keys
//    	KeyBySchemaCorrespondenceBlocker<MatchableTableRow> valueKeyBlocker = new KeyBySchemaCorrespondenceBlocker<>();
//    	valueKeyCorrespondences = valueKeyBlocker.runBlocking(valueConsolidatedKeys, true, valueBasedSchema, proc);
//    	// then consolidate the created keys, i.e., create a dataset with the new keys
//    	CandidateKeyConsolidator<MatchableTableColumn> valueKeyConsolidator = new CandidateKeyConsolidator<>();
//    	valueConsolidatedKeys = valueKeyConsolidator.run(web.getCandidateKeys(), valueKeyCorrespondences, proc);
//    	System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", 1, valueConsolidatedKeys.size(), valueKeyCorrespondences.size()));
//    	printKeys(Q.without(valueConsolidatedKeys.get(), web.getCandidateKeys().get()));
//    
////    	int vLast = valueConsolidatedKeys.size();
////    	int vRound = 2;
////    	do {
////    		vLast = valueConsolidatedKeys.size();
////    		valueConsolidatedKeys = valueKeyConsolidator.run(valueConsolidatedKeys, valueKeyCorrespondences, proc);
////    		valueKeyCorrespondences = valueKeyBlocker.runBlocking(valueConsolidatedKeys, true, valueBasedSchema, proc);
////    		System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", vRound++, valueConsolidatedKeys.size(), valueKeyCorrespondences.size()));
////    	} while(valueConsolidatedKeys.size()!=vLast);
//
//    	
//    	writeKeyGraph(valueKeyCorrespondences.get(), new File(resultsLocationFile, "v2_key_graph_2.net"));
//
//    	
//    	/*********************************************** 
//    	 * create new record links based on matching keys
//    	 ***********************************************/
//    	
//    	// match records based on matching keys
//    	MatchingKeyRecordBlocker valueMatchingKeyBlocker = new MatchingKeyRecordBlocker();
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> valueKeyInstanceCorrespondences = valueMatchingKeyBlocker.runBlocking(web.getRecords(), true, valueKeyCorrespondences, proc);
//    	writeKeyInstanceGraph(valueKeyInstanceCorrespondences.get(), new File(resultsLocationFile, "v2_instance_graph_1.net"));
//    	
//    	
//    	/*********************************************** 
//    	 * vote for schema correspondences
//    	 ***********************************************/
//    	
//    	// aggregates the votes for schema correspondences and creates the final schema correspondences
//    	InstanceByKeyToSchemaAggregator valueInstanceVoteForSchema = new InstanceByKeyToSchemaAggregator();
//    	valueBasedSchema  = valueInstanceVoteForSchema.aggregate(valueKeyInstanceCorrespondences, proc);
//    	System.out.println(String.format("Voting: %d schema correspondences", valueBasedSchema.size()));
//    	
//    	// remove special columns from schema correspondences
//    	SpecialColumnsSchemaFilter valueSpecialColumnsFilter = new SpecialColumnsSchemaFilter();
//    	valueBasedSchema = valueSpecialColumnsFilter.run(valueBasedSchema, proc);
//    	System.out.println(String.format("-> Special Columns: %d schema correspondences", valueBasedSchema.size()));
//    	
//    	// refine schema correspondences by applying disjoint header rule
//    	DisjointHeaderSchemaMatchingRule valueDisjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
//    	valueBasedSchema = valueDisjointHeaderRule.run(valueBasedSchema, proc);
//    	System.out.println(String.format("-> Disjoint Headers: %d schema correspondences", valueBasedSchema.size()));
//    	
//    	
//    	schemaMapping = N2NGoldStandard.createFromCorrespondences(valueBasedSchema.get());
//    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
//    	System.out.println("v2_schema_graph_2.net");
//    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance, false));
//    	
//    	
//    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_2.net"), web.getSchema(), valueBasedSchema);
//    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	/*********************************************** 
    	 * the duplicate-based approach would also produce all possible, correct links if all tables have the same set of attributes
    	 * however, the runtime of testing all attribute combinations is much too high
    	 * so, this implementation is an approximation and has hence a lower recall
    	 ***********************************************/
    	
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
    	writeKeyInstanceGraph(keyInstanceCorrespondences.get(), new File(resultsLocationFile, "v2_instance_graph_0.net"));
    	
    	// the notion of trusted keys is no longer necessary
    	// aggregate by table combination and determine trusted keys
    	// then remove all instance correspondences that were not created by all trusted keys
//    	TrustedKeyAggregator trustAggregator = new TrustedKeyAggregator(1);
//    	printVotesForKeys(keyInstanceCorrespondences, proc, 3, 6);
//    	keyInstanceCorrespondences = trustAggregator.aggregate(keyInstanceCorrespondences, proc);
//    	System.out.println(String.format("-- Trusted Keys: %d instance correspondences", keyInstanceCorrespondences.size()));
//    	printKeyInstanceCorrespondences(keyInstanceCorrespondences, 4, 27);
//    	printKeyInstanceCorrespondences(keyInstanceCorrespondences, -1, -1);
//    	printVotesForKeys(keyInstanceCorrespondences, proc, 3, 6);
//    	instanceMapping = N2NGoldStandard.createFromCorrespondences(keyInstanceCorrespondences.get());
//    	instancePerformance = instanceGS.evaluateCorrespondenceClusters(instanceMapping.getCorrespondenceClusters(), false);
//    	System.out.println(instanceGS.formatEvaluationResult(instancePerformance, true));
    	
    	//TODO can there still be missing transitive correspondences? 
    	// with only full key matches allowed, there are no missing transitive correspondences in the itunes case
    	// create transitive instance correspondences
//    	InstanceTransitivityAggregator instanceTransitivity = new InstanceTransitivityAggregator();
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> transitiveInstanceCorrespondences = instanceTransitivity.aggregate(keyInstanceCorrespondences, proc);
//    	System.out.println(String.format("-> Transitivity: %d instance correspondences", transitiveInstanceCorrespondences.size()));
//    	for(Correspondence<MatchableTableRow, MatchableTableKey> cor : transitiveInstanceCorrespondences.get()) {
//    		keyInstanceCorrespondences.add(cor);
//    	}
//    	System.out.println(String.format("== Total: %d instance correspondences", keyInstanceCorrespondences.size()));
    	
    	
    	// refines the instance correspondences and creates votes for schema correspondences which contain at least one key column
//    	KeyBasedRecordMatchingRule rule = new KeyBasedRecordMatchingRule();
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = rule.run(keyInstanceCorrespondences, proc);
//    	System.out.println(String.format("-- Refinement: %d instance correspondences", instanceCorrespondences.size()));

    	// use a refiner for the instance correspondences that creates new votes even for non-key attributes
//    	RecordMatchingForSchemaVotingRule recordForSchema = new RecordMatchingForSchemaVotingRule();
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> allInstanceCorrespondences = recordForSchema.run(instanceCorrespondences, proc);
//    	System.out.println(String.format("-- Refinement (all): %d instance correspondences", allInstanceCorrespondences.size()));
//    	instanceCorrespondences = allInstanceCorrespondences;
    	
//    	instanceMapping = N2NGoldStandard.createFromCorrespondences(instanceCorrespondences.get());
//    	instancePerformance = instanceGS.evaluateCorrespondenceClusters(instanceMapping.getCorrespondenceClusters(), false);
//    	System.out.println(instanceGS.formatEvaluationResult(instancePerformance, true));
    	
//    	writeInstanceGraph(instanceCorrespondences.get(), new File(resultsLocationFile, "v2_instance_graph_1.net"));
    	
    	/*********************************************** 
    	 * vote for schema correspondences
    	 ***********************************************/
    	
    	// aggregates the votes for schema correspondences and creates the final schema correspondences
    	InstanceToSchemaAggregator voteForSchema = new InstanceToSchemaAggregator();
//    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = voteForSchema.aggregate(allInstanceCorrespondences, proc);
//    	System.out.println(String.format("Voting: %d schema correspondences", schemaCorrespondences.size()));
    	
    	InstanceByKeyToSchemaAggregator instanceVoteForSchema = new InstanceByKeyToSchemaAggregator(0, false, false, false, 1.0);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences  = instanceVoteForSchema.aggregate(keyInstanceCorrespondences, proc);
    	System.out.println(String.format("Voting: %d schema correspondences", schemaCorrespondences.size()));
    	
//    	Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> instance1 = new ArrayList<>(instanceCorrespondences.get());
    	Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> schema1 = new ArrayList<>(schemaCorrespondences.get());
    	
//    	printVotesForSchemaCorrespondences(allInstanceCorrespondences.get(), -1, 27);
//    	printVotesForSchemaCorrespondences(allInstanceCorrespondences.get(), 27, -1);
//    	printVotesForSchemaCorrespondences(allInstanceCorrespondences.get(), -1, 32);
//    	printVotesForSchemaCorrespondences(allInstanceCorrespondences.get(), 32, -1);
//    	printVotesForSchemaCorrespondences(allInstanceCorrespondences.get(), 3, 6);
//    	printVotesForSchemaCorrespondences(allInstanceCorrespondences.get(), -1, -1);
    	
//    	printSchemaCorrespondences(schemaCorrespondences);
    	
    	// the schema cluster can contain errors if two tables have keys with exactly the same values in different attributes
    	// Errors: 
    	// 		- key values match, but contain different attributes (globally ambiguous key values, not detectable locally)
    	//		- more general: too few matching records, unanimous schema votes are pure coincidence
    	// Example:
    	// #4<->27
    	// 1438042990112.50_201 | 0                    | 1                    | livin' la vida loca  | ricky martin         | 4:03                 | 12,00 kr             | visa i itunes       
    	// 1438042989126.22_201 | 0                    | 1                    | livin' la vida loca  | ricky martin         | 4:03                 | 1,29 €               | ver no itunes
    	// contains two incorrect schema correspondences:
    	//		- column index 2: row number vs track number
    	//		- column index 4: album vs artist
    	// in this case, all instance-based schema matching methods will likely fail (as the records seem to be exact duplicates)
    	// also, we have no connection between the attribute labels yet or do not have attribute labels at all
    	// (overfitting?) in this case, looking at the table's position on the website would given us the essential clue ...
    	// (insufficient) in this case, using global ambiguity avoidance ('ricky martin' is known to be ambiguous) would remove that instance correspondence (but does not guarantee to remove all such correspondences) ...
    	// can we get more clues from the remaining, unmatching records?
    	// the current method, only using certain key matches, produces very few correspondences
    	// if we used partial key matches, we would get more correspondences that could vote for (parts of) the schema
    	// in the example, we could likely find a partial match for multiple (name,artist,time) combinations, which would help us establish the correct artist/album mapping 
    	
    	// however, they are not complete and hence additional clusters for the same attribute exist
    	// if we evaluate using the original tables, another cluster is chosen as best match for the gs cluster and hence we get a different result ...
//    	schemaMapping = createSchemaMappingFromCorrespondences(schemaCorrespondences.get());
    	schemaMapping = N2NGoldStandard.createFromCorrespondences(schemaCorrespondences.get());
    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), false));
    	
    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_0.net"), web.getSchema(), schemaCorrespondences);
    	
//    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> allSchemaCorrespondences = voteForSchema.aggregate(allInstanceCorrespondences, proc);
//    	System.out.println(String.format("-- Voting (all): %d schema correspondences", allSchemaCorrespondences.size()));
    	
    	// remove special columns from schema correspondences
    	SpecialColumnsSchemaFilter specialColumnsFilter = new SpecialColumnsSchemaFilter();
//    	schemaCorrespondences = specialColumnsFilter.run(schemaCorrespondences, proc);
//    	System.out.println(String.format("-> Special Columns: %d schema correspondences", schemaCorrespondences.size()));
//    	
//    	schemaMapping = createSchemaMappingFromCorrespondences(schemaCorrespondences.get());
//    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
//    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance, true));
    	
    	// refine schema correspondences by applying disjoint header rule
    	DisjointHeaderSchemaMatchingRule disjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
//    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
//    	System.out.println(String.format("-> Disjoint Headers: %d schema correspondences", schemaCorrespondences.size()));
//    	
//    	schemaMapping = createSchemaMappingFromCorrespondences(schemaCorrespondences.get());
//    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
//    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance, true));
    	
//    	printSchemaCorrespondences(schemaCorrespondences);

//    	SimilarityMatrix<MatchableTableColumn> schemaMatrix = SimilarityMatrix.fromCorrespondences(schemaCorrespondences.get(), new SparseSimilarityMatrixFactory());
//    	System.out.println(schemaMatrix.getOutput());
    	
    	
    	// alternative idea: inclusion dependency induction
    	// - find the attributes that match for all/most tables in an instance correspondence cluster
    	// - extract their values into a new relation
    	// - all existing tables from that cluster now have an inclusion dependency to the new table
    	// + we can match other tables to the new relation (advantage: fixed matching key size & more examples -> less ambiguity)
    	// + for all additional attributes, which do not match perfectly (<1.0), we can assume that there is a :n relationship
    	// + we might find multiple 'main' entities that we can extract 
    	// -> automatic schema normalisation

    	
    	/*********************************************** 
    	 * refine instance correspondences
    	 ***********************************************/
    	
    	// this correspondences are not needed, we don't use them afterwards...
    	// aggregates the schema correspondences' votes for instance correspondences and creates final instance correspondences
//    	SchemaToInstanceAggregator voteForRecord = new SchemaToInstanceAggregator();
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = voteForRecord.aggregate(schemaCorrespondences, proc);
//    	System.out.println(String.format("Voting: %d instance correspondences", instanceCorrespondences.size()));
////    	printInstanceCorrespondences(instanceCorrespondences);
//    	
//    	instanceMapping = N2NGoldStandard.createFromCorrespondences(instanceCorrespondences.get());
//    	instancePerformance = instanceGS.evaluateCorrespondenceClusters(instanceMapping.getCorrespondenceClusters(), true);
//    	System.out.println(instanceGS.formatEvaluationResult(instancePerformance, true));
//    	
    	/*********************************************** 
    	 * refine schema correspondences
    	 ***********************************************/
    	
    	// create transitive schema correspondences
//    	SchemaTransitivityAggregator schemaTransitivity = new SchemaTransitivityAggregator();
//    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> transitiveSchemaCorrespondences = schemaTransitivity.aggregate(schemaCorrespondences, proc);
//    	System.out.println(String.format("Transitivity: %d schema correspondences", transitiveSchemaCorrespondences.size()));
////    	printSchemaCorrespondences(transitiveSchemaCorrespondences);
//    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : transitiveSchemaCorrespondences.get()) {
//    		schemaCorrespondences.add(cor);
//    	}
//    	System.out.println(String.format("== Total: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// refine schema correspondences by applying disjoint header rule
//    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
//    	System.out.println(String.format("-> Disjoint Headers: %d schema correspondences", schemaCorrespondences.size()));
    	
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
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondencesFromSynonyms.get()) {
    		schemaCorrespondences.add(cor);
    	}
    	schemaCorrespondences.deduplicate();
    	System.out.println(String.format("== Total: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// remove special columns from schema correspondences (synonymBlocker is a blocker and uses all columns, so new correspondences for special columns can be created) 
    	specialColumnsFilter = new SpecialColumnsSchemaFilter();
    	schemaCorrespondences = specialColumnsFilter.run(schemaCorrespondences, proc);
    	System.out.println(String.format("-> Special Columns: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// refine schema correspondences by applying disjoint header rule
    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
    	System.out.println(String.format("-> Disjoint Headers: %d schema correspondences", schemaCorrespondences.size()));
    	
    	schemaMapping = N2NGoldStandard.createFromCorrespondences(schemaCorrespondences.get());
    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	System.out.println("v2_schema_graph_1.net");
    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), false));
    	
    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_1.net"), web.getSchema(), schemaCorrespondences);
    	
    	
    	
    	/*********************************************** 
    	 * match keys
    	 ***********************************************/

    	System.out.println(String.format("UCC: %d candidate keys", web.getCandidateKeys().size()));
    	
    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = null;
    	BasicCollection<MatchableTableKey> consolidatedKeys = web.getCandidateKeys();
    	
    	// run a blocker with schema correspondences that propagates the keys
    	KeyBySchemaCorrespondenceBlocker<MatchableTableRow> keyBlocker = new KeyBySchemaCorrespondenceBlocker<>(false);
    	keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    	// then consolidate the created keys, i.e., create a dataset with the new keys
    	CandidateKeyConsolidator<MatchableTableColumn> keyConsolidator = new CandidateKeyConsolidator<>();
    	consolidatedKeys = keyConsolidator.run(web.getCandidateKeys(), keyCorrespondences, proc);
    	System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", 1, consolidatedKeys.size(), keyCorrespondences.size()));
    	printKeys(Q.without(consolidatedKeys.get(), web.getCandidateKeys().get()));
    	
    	Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> keysBefore = null;
    	Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> keysAfter = keyCorrespondences.get();
    	Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> diff = null; 
    	
    	int last = consolidatedKeys.size();
    	int round = 2;
    	do {
    		keysBefore = keysAfter;
    		last = consolidatedKeys.size();
    		consolidatedKeys = keyConsolidator.run(consolidatedKeys, keyCorrespondences, proc);
    		keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    		System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", round++, consolidatedKeys.size(), keyCorrespondences.size()));
    		keysAfter = keyCorrespondences.get();
    		diff = Q.without(keysAfter, keysBefore);
    		printKeyCorrespondences(diff);
    	} while(consolidatedKeys.size()!=last);

    	
    	writeKeyGraph(keyCorrespondences.get(), new File(resultsLocationFile, "v2_key_graph_2.net"));

    	
    	/*********************************************** 
    	 * create new record links based on matching keys
    	 ***********************************************/
    	
    	// match records based on matching keys
    	MatchingKeyRecordBlocker matchingKeyBlocker = new MatchingKeyRecordBlocker();
    	keyInstanceCorrespondences = matchingKeyBlocker.runBlocking(web.getRecords(), true, keyCorrespondences, proc);
    	writeKeyInstanceGraph(keyInstanceCorrespondences.get(), new File(resultsLocationFile, "v2_instance_graph_1.net"));
    	
    	
    	/*********************************************** 
    	 * vote for schema correspondences
    	 ***********************************************/
    	
    	// aggregates the votes for schema correspondences and creates the final schema correspondences
    	schemaCorrespondences  = instanceVoteForSchema.aggregate(keyInstanceCorrespondences, proc);
    	System.out.println(String.format("Voting: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// remove special columns from schema correspondences
    	schemaCorrespondences = specialColumnsFilter.run(schemaCorrespondences, proc);
    	System.out.println(String.format("-> Special Columns: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// refine schema correspondences by applying disjoint header rule
    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
    	System.out.println(String.format("-> Disjoint Headers: %d schema correspondences", schemaCorrespondences.size()));
    	
    	
    	schemaMapping = N2NGoldStandard.createFromCorrespondences(schemaCorrespondences.get());
    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	System.out.println("v2_schema_graph_2.net");
    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), false));
    	
    	
    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_2.net"), web.getSchema(), schemaCorrespondences);
    	
    	
    	
    	/*********************************************** 
    	 * refine schema correspondences
    	 ***********************************************/
    	
    	// create transitive schema correspondences
//    	transitiveSchemaCorrespondences = schemaTransitivity.aggregate(schemaCorrespondences, proc);
//    	System.out.println(String.format("Transitivity: %d schema correspondences", transitiveSchemaCorrespondences.size()));
////    	printSchemaCorrespondences(transitiveSchemaCorrespondences);
//    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : transitiveSchemaCorrespondences.get()) {
//    		schemaCorrespondences.add(cor);
//    	}
//    	System.out.println(String.format("== Total: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// refine schema correspondences by applying disjoint header rule
    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
    	System.out.println(String.format("-> Disjoint Headers: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// add schema correspondences via attribute names
    	synonyms = generateSynonyms.runBlocking(web.getSchema(), true, schemaCorrespondences, proc);
    	System.out.println("Synonyms");
    	for(Set<String> clu : synonyms.get()) {
    		System.out.println("\t" + StringUtils.join(clu, ","));
    	}
    	dh.extendWithSynonyms(synonyms.get());

    	synonymBlocker = new SynonymBasedSchemaBlocker(synonyms);
    	schemaCorrespondencesFromSynonyms = synonymBlocker.runBlocking(web.getSchema(), true, null, proc);    	
    	schemaCorrespondencesFromSynonyms.deduplicate();
    	System.out.println(String.format("Synonyms: %d schema correspondences", schemaCorrespondencesFromSynonyms.size()));
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondencesFromSynonyms.get()) {
    		schemaCorrespondences.add(cor);
    	}
    	schemaCorrespondences.deduplicate();
    	System.out.println(String.format("== Total: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// remove special columns from schema correspondences (synonymBlocker is a blocker and uses all columns, so new correspondences for special columns can be created) 
    	specialColumnsFilter = new SpecialColumnsSchemaFilter();
    	schemaCorrespondences = specialColumnsFilter.run(schemaCorrespondences, proc);
    	System.out.println(String.format("-> Special Columns: %d schema correspondences", schemaCorrespondences.size()));
    	
    	// refine schema correspondences by applying disjoint header rule
    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
    	System.out.println(String.format("-> Disjoint Headers: %d schema correspondences", schemaCorrespondences.size()));
    	
    	schemaMapping = N2NGoldStandard.createFromCorrespondences(schemaCorrespondences.get());
    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	System.out.println("v2_schema_graph_3.net");
    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), false));
    	
    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_3.net"), web.getSchema(), schemaCorrespondences);
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	long end = System.currentTimeMillis();
    	
    	System.out.println(String.format("Matching finished after %s", DurationFormatUtils.formatDurationHMS(end-start)));
    	
    	System.out.println("done");
    	
    	
    	
    	
    	
    	
    	/***********************************************
    	 * Schema-Free Identity Resolution
    	 ***********************************************/

//    	WebTableKeyBlockingKeyGenerator blockingKeyGenerator = new WebTableKeyBlockingKeyGenerator();
//    	blockingKeyGenerator.setAmbiguityAvoidanceMode(AmbiguityAvoidance.Global);
//    	
//    	KeyBasedBlocker<Integer, MatchableTableRow, MatchableTableColumn, WebTableKey> keyBasedBlocker = 
//    			new KeyBasedBlocker<>(tableKeys, 
//    					new MatchableTableRow.MatchableTableRowToTableId(), 
//    					new WebTableKey.WebTableKeyToTableId(), 
//    					blockingKeyGenerator);
//    	
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> allInstanceCorrespondences =  keyBasedBlocker.runBlocking(web.getRecords(), true, null, proc);
//    	
//    	// schema matching
//    	DefaultDataSet<MatchableTableColumn, MatchableTableColumn> schema = new DefaultDataSet<>();
//    	Map<Integer, Map<Integer, String>> tableColumnIdentifiers = new HashMap<>(); // tableId columnIndex columnId
//    	for(MatchableTableColumn c : web.getSchema().get()) {
//    		if(!SpecialColumns.ALL.contains(web.getColumnHeaders().get(c.getIdentifier()))) {
//    			schema.add(c);
//    			Map<Integer, String> tblMap = MapUtils.get(tableColumnIdentifiers, c.getTableId(), new HashMap<Integer, String>());
//    			tblMap.put(c.getColumnIndex(), c.getIdentifier());
//    		}
//    	}
//    	
//		DuplicateBasedWebTableSchemaMatchingRule rule = new DuplicateBasedWebTableSchemaMatchingRule(1.0);
//		rule.setTableColumnIdentifiers(tableColumnIdentifiers);
//		
//		// run the schema matching
//		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> allSchemaCorrespondences = matchingEngine.runDuplicateBasedSchemaMatching(schema, schema, allInstanceCorrespondences, rule);
//    	
    	
    	// key generator
    	
    	// identity resolution
    	
    	
    	//TODO convert Schema-Free Identity Resolution into a blocker
    	SchemaFreeIdentityResolution sfir = new SchemaFreeIdentityResolution();
    	ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs = sfir.run(web, proc, tableToCandidateKeys);
    	
    	printValueLinkStatistics(blockedPairs, proc, 1);
    	
    	int before = blockedPairs.size();
    	blockedPairs = verifyValueLinkTransitivity(blockedPairs, proc, tableToSchema, tableToCandidateKeys);
    	int after = blockedPairs.size();
    	System.out.println(String.format("&&&& Removed %d links via transitivity checks", before-after));
    	
    	System.out.println("%%% removing untrusted value links %%%");
    	blockedPairs = removeUntrustedValueLinks(blockedPairs, proc, tableToSchema, tableToCandidateKeys);
    	
    	printValueLinkStatistics(blockedPairs, proc, 0);
    	
    	System.out.println(String.format("\t%d table-to-table links", blockedPairs.size()));
    	printValueBasedLinks(blockedPairs, proc, tableToSchema, tableToCandidateKeys);
    	
    	blockedPairs = proc.sort(blockedPairs, new Function<String, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(
					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> input) {
				return input.getKey();
			}
    		
    	});
    	
    	// blockedPairs contains all the table-to-table links
    	for(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> group : blockedPairs.get()) {
    		// determine the tables from the grouping key
    		String[] ids = group.getKey().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		Table t1 = web.getTables().get(id1);
    		Table t2 = web.getTables().get(id2);   	

    		// only add the correspondences if they are new (they will be encountered in both directions: a->b and b->a)
    		if(wtmg.getInstanceCorrespondences(t1, t2)==null) {
    			
    			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = new ResultSet<>();
    			
	    		// iterate over all instance correspondences
	    		for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> link : group.getRecords().get()) {
	    			// add the links between the instances
	    			correspondences.add(new Correspondence<MatchableTableRow, MatchableTableColumn>(link.getFirst().getRow(), link.getSecond().getRow(), 1.0, null));
	    		}
	    		//TODO up to this point, all can go to the blocker
	    		//TODO the following should be a matcher using the blocker's result
	    		// -- the schema matching part can also work on all schemas at the same time, if the special columns are removed beforehand
	    		// --maybe matching key generation and identity resolution can be performed in a later step ... initial matching keys are not that reliable anyway ...
	    		TableToTableMatcher t2t = new TableToTableMatcher();
	    		t2t.match(t1, t2, correspondences, web, matchingEngine, proc);
	    		
	    		//TODO this should be t2t.getInstanceMapping() instead of correspondences !!!
//	    		wtmg.addInstanceCorrespondence(t1, t2, correspondences);
	    		int instBefore = correspondences.size();
	    		wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
	    		int instAfter = t2t.getInstanceMapping()==null ? 0 : t2t.getInstanceMapping().size();
	    		
	    		if(instBefore-instAfter!=0) {
	    			System.out.println(String.format("Removed %d instance correspondences", instBefore-instAfter));
	    		}
	    		
	    		//if(t2t.getInstanceMapping()!=null && t2t.getInstanceMapping().size()>0) {
	    		if(t2t.getMatchingKeys()!=null) {
	    			wtmg.addSchemaCorrespondences(t1, t2, t2t.getSchemaMapping());
//	    			wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
	    			wtmg.addMatchingKeys(t1, t2, t2t.getMatchingKeys());
	    		}
    		}
    	}
    	
    	/** print matching graph **/
    	System.out.println(wtmg.formatMatchingGraph());
    	wtmg.writeMatchingKeyGraph(new File(resultsLocationFile, "c3_keygraph_1.net"));
    	wtmg.writeInstanceCorrespondenceGraph(new File(resultsLocationFile, "c3_instancegraph_1.net"));
    	wtmg.writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "c3_schemagraph_1.net"), web.getSchema());
    	wtmg.writeSchemaCorrespondenceGraphForMatchingKeys(new File(resultsLocationFile, "c3_keyschemagraph_1.net"), web.getSchema());
    	wtmg.writeSchemaCorrespondenceGraphByTable(new File(resultsLocationFile, "c3_tableschemagraph_1.net"));
    	
		// after matching the schemas of all tables, propagate information about impossible matches in the graph
		// - if a schema correspondence cannot be created between two tables, there cannot be a path via other tables that represents the same correspondence
    	
    	// tables A, B, C
    	// keys: {nr},{name}
    	// join: A.nr <-> B.nr
    	// join B.nr <-> C.nr 
    	// join A.name <-> C.name
    	// -> A.nr <-> C.nr <- if this causes a conflict, there is an error in the graph of join keys!
    	// -- a conflict exists if there is an edge by instance correspondences but no edge by a schema correspondence
    	// -- apply transitivity rule to all schema correspondences in the graph and check for conflicts (schema correspondences already contain the conflicts, as similarity is proportional to the relative number of conflicts)
    	// -- -> all transitive edges already exist, only need to check similarity value??? 
    	
    	// for each cluster in the graph of join keys
    	//  for each table in the cluster
    	//    materialise transitivity for schema correspondences:
    	//		for each pair of an incoming arc and an outgoing arc which share the same column of the current table
    	//		  create a (virtual) schema correspondence in the two connected tables
    	//		  check these correspondences for conflicts (they are already in the set of schema correspondences with a certain similarity, otherwise no record voted for this combination)
    	//	      if there is a conflict (schema correspondence score != 1.0), mark the correspondence as conflicting
    	//        mark the join keys that include the columns from the correspondence as conflicting
    	//		  propose the schema correspondence with maximum similarity as additional key
    	//		  propagate the conflict state along all edges with the conflicting join keys
    	
    	// keys must be extended! find non-conflicting, larger keys (don't try to merge the conflicting keys, as they conflict)
    	// -- we can add any attribute to the conflicting candidate keys to make them larger
    	// -- this is valid as keys are discovered from data and we expect them to be under-estimated
    	// -- if we use the schema correspondence with the highest similarity value, we know that the new key will be the one with the minimum number of additional conflicts
    	// -- if no additional schema correspondences exist, remove the connection completely
    	// check existing instance correspondences for subsets that are consistent w.r.t. a larger key
    	// basically, use result from duplicate-based schema matching and the attributes from the correspondence with the highest similarity < 1 to the key
    	// check if the graph is conflict free with the correspondences created from the new keys (use subset of instance correspondences to check for conflicts)
    	
    	/***********************************************
    	 * Add correspondences via transitivity
    	 ***********************************************/
    	
//    	int ttlCor1 = wtmg.countSchemaCorrespondences();
    	System.out.println("***** Materialising Schema Correspondence Transitivity *****");
//    	int transitivityCount = 
		wtmg.materialiseSchemaCorrespondenceTransitivity();
    	wtmg.writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "c3_schemagraph_2_transitivity.net"), web.getSchema());
    	wtmg.writeSchemaCorrespondenceGraphForMatchingKeys(new File(resultsLocationFile, "c3_keyschemagraph_2_transitivity.net"), web.getSchema());
    	wtmg.writeSchemaCorrespondenceGraphByTable(new File(resultsLocationFile, "c3_tableschemagraph_2_transitivity.net"));
    	//TODO also propagate candidate keys here?
//    	int ttlCor2 = wtmg.countSchemaCorrespondences();
    	
    	//TODO refactor the following block as a method in the web tables matching graph
    	// now check all instance correspondences
    	List<Table> tablesWithCors = wtmg.getTablesWithSchemaCorrespondences();
//    	for(Table t1 : tablesWithCors) {
//    		List<Table> connectedTables = wtmg.getTablesConnectedViaSchemaCorrespondences(t1);
//    		
//    		for(Table t2 : connectedTables) {
//    			
////    			if(wtmg.getInstanceCorrespondences(t1, t2)!=null) {
//    			
//    			// TODO we re-create all matching keys and instance correspondences here anyway, so no need to calculate them earlier
//    			
//	    			TableToTableMatcher t2t = new TableToTableMatcher();
//	    			t2t.runIdentityResolution(t1, t2, wtmg.getInstanceCorrespondences(t1, t2), wtmg.getSchemaCorrespondences(t1, t2), web, matchingEngine, proc);
//	    			
//	    			wtmg.removeInstanceCorrespondences(t1, t2);
//	    			wtmg.removeMatchingKeys(t1, t2);
//	    			
//	    			if(t2t.getMatchingKeys()!=null) {
//	    				wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
//	    				wtmg.addMatchingKeys(t1, t2, t2t.getMatchingKeys());
//	    			} else {
//	    				System.out.println("No Matching Key");
//	    			}
////    			}
//    			
//    		}
//    	}
    	
    	// merge tables and write
    	int nextTableId;
    	WebTablesMatchingGraph fused;
    	Collection<Table> fusedTables;
    	
//    	nextTableId = Q.max(web.getTables().keySet()) + 1;
//    	fused = wtmg.fuseTables(nextTableId);
//    	fusedTables = fused.getAllTables();
//    	System.out.println("*** Fused Matching Graph ***");
//    	System.out.println(fused.formatMatchingGraph());
//    	WebTables.writeTables(fusedTables, resultsLocationFile, csvResultsLocationFile);
    	
    	
    	/***********************************************
    	 * Add correspondences via attribute names
    	 ***********************************************/
		// then, generate synonym sets from all schema correspondences
		// use the synonym sets to resolve the conflicts that were created earlier
    	// - only use synonyms that were created from non context-dependent columns
    	//TODO -- > add context-dependent flag to TableColumn and propagate it in the graph of schema correspondences
		// - if the column headers are synonyms (and at least one row supports the correspondence), create it 
		// - remove all instance correspondences that create conflicts with the new schema correspondences
    	
    	System.out.println("***** Adding Schema Correspondence via Attribute Names *****");
//    	int namesCount = 
		wtmg.addSchemaCorrespondencesViaAttributeNames(web.getTables().values());
    	wtmg.writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "c3_schemagraph_3_synonyms.net"), web.getSchema());
    	wtmg.writeSchemaCorrespondenceGraphForMatchingKeys(new File(resultsLocationFile, "c3_keyschemagraph_3_synonyms.net"), web.getSchema());
    	wtmg.writeSchemaCorrespondenceGraphByTable(new File(resultsLocationFile, "c3_tableschemagraph_3_synonyms.net"));
//    	int ttlCor3 = wtmg.countSchemaCorrespondences();
    	
    	//TODO find a way to add the missing correspondences (no transitivity, different names, but same attribute)
    	// likely matching if: part of a candidate key, always followed by the same columns, which are also part of a candidate key 
    	// (keys tend to be on the left of the table)
    	// usually, left of keys we only find other keys, or row numbers/ranks
//    	wtmg.addSchemaCorrespondencesViaColumnPositions();
    	
    	// has no effect here ... why?
//    	fused.materialiseMatchingKeyTransitivity();
    	
    	// now check all instance correspondences
//    	tablesWithCors = wtmg.getTablesWithSchemaCorrespondences();
//    	for(Table t1 : tablesWithCors) {
//    		List<Table> connectedTables = wtmg.getTablesConnectedViaSchemaCorrespondences(t1);
//    		
//    		for(Table t2 : connectedTables) {
//    			
//    			// if no instance correspondences exist, we cannot remove any incorrect ones ...
//    			// otherwise, we can also use the initial correspondences here
//    			if(wtmg.getInstanceCorrespondences(t1, t2)!=null) {
//	    			TableToTableMatcher t2t = new TableToTableMatcher();
//	    			t2t.runIdentityResolution(t1, t2, wtmg.getInstanceCorrespondences(t1, t2), wtmg.getSchemaCorrespondences(t1, t2), web, matchingEngine, proc);
//	    			
//	    			wtmg.removeInstanceCorrespondences(t1, t2);
//	    			wtmg.removeMatchingKeys(t1, t2);
//	    			
//	    			if(t2t.getMatchingKeys()!=null) {
//	    				wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
//	    				wtmg.addMatchingKeys(t1, t2, t2t.getMatchingKeys());
//	    			} else {
//	    				System.out.println("No Matching Key");
//	    			}
//    			}
//    			
//    		}
//    	}
    	
//    	/** print modified matching graph **/
//    	System.out.println(wtmg.formatMatchingGraph());
//    	
//    	System.out.println(String.format("Schema Correspondence Count: %d", ttlCor1));
//    	System.out.println(String.format("Added %d correspondences via transitivity", transitivityCount));
//    	System.out.println(String.format("Schema Correspondence Count: %d", ttlCor2));
//    	System.out.println(String.format("Added %d correspondences via attribute names", namesCount));
//    	System.out.println(String.format("Schema Correspondence Count: %d", ttlCor3));
    	
//    	System.out.println("***** Frequent Item Sets for Column Indices *****");
//    	wtmg.addSchemaCorrespondencesViaColumnIndices();
    	
    	/***********************************************
    	 * Materialise Matching Key Transitivity
    	 ***********************************************/
    	
    	System.out.println("***** Materialising Matching Key Transitivity *****");
    	//wtmg.materialiseMatchingKeyTransitivity();
    	wtmg.propagateMatchingKeys();
    	System.out.println(wtmg.formatMatchingGraph());
    	wtmg.writeMatchingKeyGraph(new File(resultsLocationFile, "c3_keygraph_2_propagated.net"));
		
    	//TODO not only propagate matching keys, also update dependencies ... possible?
//    	wtmg.removeInvalidCandidateKeys();
    	
		// if A is a key of t1 and all columns of A are mapped to t2, but they are not a key nor do they overlap with a key of t2, 
		// then they are not unique in t2 and we cannot use them to merge the schemas without making errors.
		
		// as A is not unique in t2, we must be able to find a value of t2's keys in t1, otherwise there is no possibility for matching keys
		
		// Further, we don't know how to extend them (otherwise we would have done so above).
		// So, to be safe, we remove the keys such that no instance correspondences between the tables can be created
		//TODO remove invalid keys
		// that would also remove complete and correct keys in a 1:n relationship ...
		
    	
    	//TODO t1 with key {a} and t2 with key {b,c} won't change by propagating matching keys, as their existing keys do not overlap
    	// propagate {b,c} if it is completely mapped? - only needed if {b,c} is *not* a key in t1. But then we have to extend it to a key...
    	// ---> if {b,c} is not a key in t1, it is an underestimated key in t2, so we have to find a better key. But if we extend it with an unmapped column, we cannot check the new key
    	// in this case, we would need to find the value of {a} in t2 to get a unique key and be able to map {a}U{b,c}
    	// -- but, as we don't have this schema correspondence from duplicate-based schema matching, we know that the records in tables t1 and t2 do not overlap
    	// -- can we be sure that this combination was checked? the keys might not have been compared as they don't match in size ...
    	// so, again, we cannot make use of the keys to merge the schemas

    	// easier: propagate *not* keys in the graph: if any table has key {a,b}, then {a} alone should not be a key used for matching (keep the correspondences, but don't create matching keys)
    	// should we remove the key {a} completely?

    	// TODO if matching keys are to small, and instance correspondences cause a conflict in mapped columns, these columns must become part of the key!
//    	  Schema Correspondences:
//    		   (1,00000000) [2] side e titles <-> [2] side 2 titles
//    		   (1,00000000) [4] length <-> [4] length
//    		   (1,00000000) [3] artist <-> [3] artist
//    		  Matching Keys:
//    		   {side e titles} <-> {side 2 titles}
//    		   1438042989018.40_201 | 1                    | 2. i shot the sherif | bob marley & the wai | 05:25               
//    		   1438042989301.17_201 | 1                    | 2. i shot the sherif | bob marley           | 05:07               
    	
    	// now check all instance correspondences
//    	tablesWithCors = wtmg.getTablesWithSchemaCorrespondences();
//    	for(Table t1 : tablesWithCors) {
//    		List<Table> connectedTables = wtmg.getTablesConnectedViaSchemaCorrespondences(t1);
//    		
//    		for(Table t2 : connectedTables) {
//
//    			
//    			
//    			
//    			if(wtmg.getInstanceCorrespondences(t1, t2)!=null) {
//	    			TableToTableMatcher t2t = new TableToTableMatcher();
//	    			t2t.runIdentityResolution(t1, t2, wtmg.getInstanceCorrespondences(t1, t2), wtmg.getSchemaCorrespondences(t1, t2), web, matchingEngine, proc);
//	    			
//	    			wtmg.removeInstanceCorrespondences(t1, t2);
//	    			wtmg.removeMatchingKeys(t1, t2);
//	    			
//	    			if(t2t.getMatchingKeys()!=null) {
//	    				wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
//	    				wtmg.addMatchingKeys(t1, t2, t2t.getMatchingKeys());
//	    			} else {
//	    				System.out.println("No Matching Key");
//	    			}
//    			}
//    			
//    		}
//    	}
    	
    	//TODO start over again at this point: now new keys can be compared which will find correspondences that have not been checked before
    	
    	// TODO from the beginning to this point, it should be sufficient to run identity resolution only once
    	// -- instance correspondences do affect the schema matching, wrong instance correspondences prevent possibly correct schema correspondences
    	// -- -- with a wrong instance correspondence, the number of votes needed for 100% similarity is higher than the number of votes that can be cast by the correct instance correspondences
    	// -- instance correspondences do not affect the schema inference
    	// -- instance correspondences do not affect the matching key generation
    	// TODO after running identity resolution, we should run duplicate-based schema matching again
    	// -- after duplicate-based schema matching, identity resolution needs not be run again, as only perfectly matching schema correspondences are created
    	// -- but, applying transitivity and synonyms could lead to new schema correspondences that invalidate existing instance correspondences
    	// -- however, we never have to look for new instance correspondences?
    	// -- -- partially true, as all keys are underestimated and we have all possible correspondences for these keys
    	// -- -- but, we might have missed some correspondences in schema-free identity resolution as we only compared keys of equal size
    	// -- -- so we have to re-check table combinations with updated key sizes ...
    	// TODO one option would be to run until we can propagate the key information and then start over with the new keys, looping until no keys change anymore (but keep schema correspondences)
    	// -- run 
    	// 														--- algorithm (1) ---
    	//					A method to perform instance and schema matching on any number of tables with no prior knowledge about the data
    	// -- -- Schema-free Identity Resolution (sfir)							blocking, gives initial instance correspondences
    	// -- -- Duplicate-based Schema Matching (dbsm)							initial schema matching
    	// -- -- Join-based Identity Resolution (jbir)							identity resolution for the keys, removes obvious errors
    	// 														--- algorithm (1) ---
    	// -- -- Materialise Schema Correspondence Transitivity (msct)			adds more schema correspondences
    	// -- -- Schema Matching via Attribute Synonyms (smas)					adds more schema correspondences
    	// -- -- Propagate Matching Keys (pmk)									updates the keys
    	// TODO if an instance correspondence matches on the key values, but not on other values, it will survive identity resolution
    	// -- the data conflict then shows that the keys in the tables & the matching key are underestimated -- OR -- that there is a 1:n relationship
    	// -- in case of a 1:n relationship, we should find multiple instance correspondences for the n table and one for the 1 table (can only happen when using partial matching keys)
    	// -- in case of an underestimated key, we should find only one instance correspondence per table (otherwise, the key would not be unique in the tables)
    	
    	/** print modified matching graph **/
//    	System.out.println(wtmg.formatMatchingGraph());
    	wtmg.writeMatchingKeyGraph(new File(resultsLocationFileFuzzy.getParentFile(), "join_union_fuzzy.net"));
    	
//    	nextTableId = Q.max(web.getTables().keySet()) + 1;
//    	fused = wtmg.fuseTables(nextTableId);
//    	fusedTables = fused.getAllTables();
//    	System.out.println("*** Fused Matching Graph ***");
//    	System.out.println(fused.formatMatchingGraph());
//    	fusedTables = wtmg.fuseTables(nextTableId);
//    	WebTables.writeTables(fusedTables, resultsLocationFileFuzzy, csvResultsLocationFileFuzzy);
    	
    	
    	/***********************************************
    	 * Merge the fused graph by partial-key matching
    	 ***********************************************/
    	System.out.println("### Partial-Key Matching ###");
    	
    	ResultSet<WebTableKey> partialKeys = new ResultSet<>();
    	
    	//TODO this works great, now refactor and run it on all tables at the same time instead of one table pair after the other
    	// now check all instance correspondences
    	tablesWithCors = wtmg.getTablesWithSchemaCorrespondences();
    	//TODO following code only temporarily commented out (too slow)
//    	for(Table t1 : tablesWithCors) {
//    		List<Table> connectedTables = wtmg.getTablesConnectedViaSchemaCorrespondences(t1);
//    		
//    		for(Table t2 : connectedTables) {
//    			
//
//				// avoid generating unnecessary keys
//				/*
//				  Matching Keys:
//				   {artist,price} <-> {title,artist,price}   <--- both are full candidate keys
//				   the following keys are no necessary:
//   				   {price} <-> {title,artist,genre,price}
//				   {price} <-> {title,genre,price}
//				   {price} <-> {title,date added,artist,price}
//				   {price} <-> {title,date added,price}
//				*/
//    			
//    			//TODO iteratively propagating the candidate keys means we have to continue until no changes occur anymore ... can we solve it cluster-based?
//    			
//    	    	WebTableKeyBlockingKeyGenerator blockingKeyGenerator = new WebTableKeyBlockingKeyGenerator();
//    	    	blockingKeyGenerator.setAmbiguityAvoidanceMode(AmbiguityAvoidance.Global);
//    	    	
//    	    	MatchingKeyGenerator mkg = new MatchingKeyGenerator();
//				Set<WebTableMatchingKey> subkeys 
//				= mkg.generateMaximalSubJoinKeysFromCorrespondences(t1, t2, wtmg.getSchemaCorrespondences(t1, t2), 1.0);
//    	    	
//				if(subkeys!=null) {
//	    	    	ResultSet<WebTableKey> keys = new ResultSet<>();
//	    	    	for(WebTableMatchingKey k : subkeys) {
//	    	    		keys.add(new WebTableKey(t1.getTableId(), new HashSet<>(Q.project(k.getFirst(), new TableColumn.ColumnIndexProjection()))));
//	    	    		keys.add(new WebTableKey(t2.getTableId(), new HashSet<>(Q.project(k.getSecond(), new TableColumn.ColumnIndexProjection()))));
//	    	    	}
//	    	    	
//	    	    	KeyBasedBlocker<Integer, MatchableTableRow, MatchableTableColumn, WebTableKey> keyBasedBlocker = 
//	    	    			new KeyBasedBlocker<>(keys, 
//	    	    					new MatchableTableRow.MatchableTableRowToTableId(), 
//	    	    					new WebTableKey.WebTableKeyToTableId(), 
//	    	    					blockingKeyGenerator);
//
//	    	    	ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> allInstanceCorrespondences =  keyBasedBlocker.runBlocking(web.getRecords(), true, null, proc);
//	    			
//	    	    	if(allInstanceCorrespondences.size()>0) {
//	    	    		// we found overlapping instances based on the new keys
//	    	    	
//	    	    		Table tab1, tab2;
//	    	    		
//	    	    		// we don't know in which direction the correspondences were created, so we must check it
//	    	    		int t1Id = allInstanceCorrespondences.get().iterator().next().getFirstRecord().getTableId();
//	    	    		if(t1.getTableId()==t1Id) {
//	    	    			tab1 = t1;
//	    	    			tab2 = t2;
//	    	    		} else {
//	    	    			tab1 = t2;
//	    	    			tab2 = t1;
//	    	    		}
//	    	    		
//		    	    	TableToTableMatcher t2t = new TableToTableMatcher();
//		    			t2t.match(tab1, tab2, allInstanceCorrespondences, web, matchingEngine, proc);
//		    	    	
//		    			int corsBefore = wtmg.getSchemaCorrespondences(tab1, tab2).size();
//		    			int corsAfter = t2t.getSchemaMapping().size();
//		    			if(corsBefore<corsAfter) {
//		    				System.out.println(String.format("@@@ found %d new schema correspondences", corsAfter - corsBefore));
//		    				HashSet<Correspondence<MatchableTableColumn, MatchableTableRow>> before = new HashSet<>(wtmg.getSchemaCorrespondences(tab1, tab2).get());
//		    				HashSet<Correspondence<MatchableTableColumn, MatchableTableRow>> after = new HashSet<>(t2t.getSchemaMapping().get());
//		    				
//		    				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : after) {
//		    					if(!before.contains(cor)) {
//		    						System.out.println(String.format("\t{%d}[%d]%s <-> {%d}[%d]%s", 
//		    								cor.getFirstRecord().getTableId(),
//		    								cor.getFirstRecord().getColumnIndex(),
//		    								web.getColumnHeaders().get(cor.getFirstRecord().getIdentifier()),
//		    								cor.getSecondRecord().getTableId(),
//		    								cor.getSecondRecord().getColumnIndex(),
//		    								web.getColumnHeaders().get(cor.getSecondRecord().getIdentifier())
//		    								));
//		    						System.out.println(String.format("\t\t%s <-> %s", cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()));
//		    					}
//		    				}
//		    				
//			    			wtmg.removeInstanceCorrespondences(tab1, tab2);
//			    			wtmg.addInstanceCorrespondence(tab1, tab2, t2t.getInstanceMapping());
//			    			
//			    			wtmg.addSchemaCorrespondences(tab1, tab2, t2t.getSchemaMapping());
//			    			
//			    			if(t2t.getMatchingKeys()!=null) {
//			    				wtmg.addMatchingKeys(tab1, tab2, t2t.getMatchingKeys());
//			    				
//			    				for(WebTableMatchingKey key : t2t.getMatchingKeys()) {
//			    					System.out.println(String.format("~~~~Key: {%s}{%s} <-> {%s}{%s}", tab1.getPath(), Q.project(key.getFirst(), new TableColumn.ColumnHeaderProjection()),
//			    							tab2.getPath(), Q.project(key.getSecond(), new TableColumn.ColumnHeaderProjection())));
//			    				}
//			    			}
//			    			
//			    			wtmg.addSchemaCorrespondencesViaAttributeNames(web.getTables().values());
//							wtmg.materialiseSchemaCorrespondenceTransitivity();
//		    			}
//	
//	    	    	}
//				}
//    		}
//    	}
    	
    	/***********************************************
    	 * Match partial keys to obtain full key matches
    	 ***********************************************/
    	
//    	WebTableKeyBlockingKeyGenerator partialKeyGenerator = new WebTableKeyBlockingKeyGenerator();
//    	blockingKeyGenerator.setAmbiguityAvoidanceMode(AmbiguityAvoidance.Global);
//    	
//    	KeyBasedBlocker<Integer, MatchableTableRow, MatchableTableColumn, WebTableKey> partialKeyBasedBlocker = 
//    			new KeyBasedBlocker<>(partialKeys, 
//    					new MatchableTableRow.MatchableTableRowToTableId(), 
//    					new WebTableKey.WebTableKeyToTableId(), 
//    					partialKeyGenerator);
//    	
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondencesFromPartialKeys =  partialKeyBasedBlocker.runBlocking(web.getRecords(), true, null, proc);
//    	
//    	ResultSet<Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>>>  groupedPartialKeyInstances = proc.groupRecords(instanceCorrespondencesFromPartialKeys, new RecordKeyValueMapper<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableColumn> record,
//					DatasetIterator<Pair<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {
//				
//				String key = String.format("%d-%d", Math.min(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), Math.max(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()));
//				
//				resultCollector.next(new Pair<String, Correspondence<MatchableTableRow,MatchableTableColumn>>(key, record));
//				
//			}
//		});
//    	
//    	for(Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>> g : groupedPartialKeyInstances.get()) {
//    		String[] ids = g.getKey().split("-");
//    		
//    		int t1Id = Integer.parseInt(ids[0]);
//    		int t2Id = Integer.parseInt(ids[1]);
//    		
//    		Table t1 = web.getTables().get(t1Id);
//    		Table t2 = web.getTables().get(t2Id);
//    		
//    		TableToTableMatcher t2t = new TableToTableMatcher();
//    		
//    		t2t.match(t1, t2, g.getRecords(), web, matchingEngine, proc);
//    		
//    		
//    		fused.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
//    		
//    		//if(t2t.getInstanceMapping()!=null && t2t.getInstanceMapping().size()>0) {
//    		if(t2t.getMatchingKeys()!=null) {
//    			fused.addSchemaCorrespondences(t1, t2, t2t.getSchemaMapping());
////    			wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
//    			fused.addMatchingKeys(t1, t2, t2t.getMatchingKeys());
//    		}
//    	}
    	
    	// works here, but it's too late as the rows with incorrect correspondences are already merged
//    	fused.materialiseMatchingKeyTransitivity();
    	
    	wtmg.writeMatchingKeyGraph(new File(resultsLocationFile, "c3_keygraph_3_subkey.net"));
    	wtmg.writeInstanceCorrespondenceGraph(new File(resultsLocationFile, "c3_instancegraph_2_subkey.net"));
    	wtmg.writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "c3_schemagraph_4_subkey.net"), web.getSchema());
    	wtmg.writeSchemaCorrespondenceGraphForMatchingKeys(new File(resultsLocationFile, "c3_keyschemagraph_4_subkey.net"), web.getSchema());
    	wtmg.writeSchemaCorrespondenceGraphByTable(new File(resultsLocationFile, "c3_tableschemagraph_4_subkey.net"));
    	
    	/***********************************************
    	 * Matching key statistics
    	 ***********************************************/
    	// express candidate keys in terms of schema clusters
    	Set<Collection<TableColumn>> schemaClusters = wtmg.getAttributeClusters(new ArrayList<>(web.getTables().values()));
    	HashMap<TableColumn, Collection<TableColumn>> schemaClusterIndex = new HashMap<>();
    	for(Collection<TableColumn> cluster : schemaClusters) {
    		for(TableColumn c : cluster) {
    			schemaClusterIndex.put(c, cluster);
    		}
    	}
    	
    	HashMap<Set<Collection<TableColumn>>, Integer> clusterKeyCounts = new HashMap<>();
    	
    	for(Table t : web.getTables().values()) {
    		
    		for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {

    			Set<Collection<TableColumn>> clusterKey = new HashSet<>();
    			for(TableColumn c : key) {
    				clusterKey.add(schemaClusterIndex.get(c));
    			}
    		
    			MapUtils.increment(clusterKeyCounts, clusterKey);
    		}
    	}
    	
    	System.out.println("*** Clustered Candidate Keys");
    	for(Set<Collection<TableColumn>> clusterKey : clusterKeyCounts.keySet()) {
    		List<String> names = new ArrayList<>(clusterKey.size());
    		
    		for(Collection<TableColumn> cluster : clusterKey) {
    			Distribution<String> headerDist = Distribution.fromCollection(Q.project(cluster, new TableColumn.ColumnHeaderProjection()));
    			names.add(headerDist.getMode());
    		}
    		
    		String name = String.format("{%s}", StringUtils.join(names, ","));
    		
    		System.out.println(String.format("%d\t%s", clusterKeyCounts.get(clusterKey), name));
    	}
    	
    	nextTableId = Q.max(web.getTables().keySet()) + 1;
    	System.out.println("*** Matching Graph with partial key matching ***");
    	System.out.println(wtmg.formatMatchingGraph());
//    	fused = fused.fuseTables(nextTableId);
//    	fusedTables = fused.getAllTables();
//    	System.out.println("*** Fused Matching Graph ***");
//    	System.out.println(fused.formatMatchingGraph());
    	
    	fused = wtmg.fuseTables(nextTableId);
    	fusedTables = fused.getAllTables();
    	WebTables.writeTables(fusedTables, resultsLocationFileFuzzySubKey, csvResultsLocationFileFuzzySubKey);
    	TableReport report = new TableReport();
    	report.writeTableReport(fusedTables, new File(resultLocation, "tablereport_propagated.txt"));

    	// can't do this, too expensive ...
    	WebTables.calculateDependenciesAndCandidateKeys(fusedTables, csvResultsLocationFileFuzzySubKey);
//    	fused.checkCandidateKeyUniqueness();
    	report = new TableReport();
    	report.writeTableReport(fusedTables, new File(resultLocation, "tablereport_recalculated.txt"));
    	
    	/***********************************************
    	 * Write Correspondences & Evaluate
    	 ***********************************************/
    	// write all provenance data for gold standard creation and evaluation
    	N2NGoldStandard n2n = new N2NGoldStandard();
    	HashSet<String> allTables = new HashSet<>();
    	for(Table t : fusedTables) {
    		
    		for(TableColumn c : t.getColumns()) {
    			
    			Set<String> clu = new HashSet<>();
    			
    			if(!SpecialColumns.isSpecialColumn(c)) {
    				
    				clu.addAll(c.getProvenance());
    			
    				for(String prov : c.getProvenance()) {
    					allTables.add(prov.split(";")[0]);
    				}
//    				allTables.addAll(c.getProvenance());
    			}
    			
    			n2n.getCorrespondenceClusters().put(clu, String.format("{%s}[%d]%s", t.getPath(), c.getColumnIndex(), c.getHeader()));
    			
    		}
    		
    	}
    	
    	n2n.writeToTSV(new File(evaluationLocation, "correspondences_join_union.tsv"));
    	
    	N2NGoldStandard gs = new N2NGoldStandard();
    	File gsFile = new File(evaluationLocation, "goldstandard.tsv");
    	if(gsFile.exists()) {
	    	gs.loadFromTSV(gsFile);

	    	ClusteringPerformance perf = gs.evaluateCorrespondenceClusters(n2n.getCorrespondenceClusters(), false);
	    
//	    	double weightedPrecisionSum = 0.0;
//	    	double weightedRecallSum = 0.0;
//	    	double weightedF1Sum = 0.0;
//	    	int count = 0;
	    	
	    	System.out.println("*** Evaluation ***");
	    	System.out.println(String.format("\t%d tables in all correspondences", allTables.size()));
	    	System.out.println(gs.formatEvaluationResult(perf.getPerformanceByCluster(), false));
//	    	for(String key : perf.keySet()) {
//	    		Performance p = perf.get(key);
//	    		System.out.println(String.format("%s:\tprec: %.4f\trec:%.4f\tf1:%.4f\t\t%d/%d correspondences", org.apache.commons.lang3.StringUtils.rightPad(key, 30), p.getPrecision(), p.getRecall(), p.getF1(), p.getNumberOfPredicted(), p.getNumberOfCorrectTotal()));
//	    		
//	    		weightedPrecisionSum += p.getPrecision() * p.getNumberOfCorrectTotal();
//	    		weightedRecallSum += p.getRecall() * p.getNumberOfCorrectTotal();
//	    		weightedF1Sum += p.getF1() * p.getNumberOfCorrectTotal();
//	    		count += p.getNumberOfCorrectTotal();
//	    	}
//	    	
//	    	System.out.println("Weighted Mean Performance");
//	    	System.out.println(String.format("\t%s %.4f", org.apache.commons.lang3.StringUtils.rightPad("Precision:",10), weightedPrecisionSum / count));
//	    	System.out.println(String.format("\t%s %.4f", org.apache.commons.lang3.StringUtils.rightPad("Recall:",10), weightedRecallSum / count));
//	    	System.out.println(String.format("\t%s %.4f", org.apache.commons.lang3.StringUtils.rightPad("F1:",10), weightedF1Sum / count));
    	}
    	
    	//TODO evaluate performance of T2K mappings
    }

	/***
	 * 
	 * @param blockedPairs
	 * @param proc
	 * @param tableToSchema
	 * @param tableToCandidateKeys
	 * @return a map table->table->key->mapped rows
	 */
	private Map<Table, Map<Table, Map<String, Set<Integer>>>> printValueBasedLinks(
			ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs, 
			DataProcessingEngine proc,
			final HashMap<Integer, String> tableToSchema,
			final HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys) {
		
		// sort all the groups by (from table),(to table)
    	System.out.println("sorting");
//    	blockedPairs = proc.sort(blockedPairs, new Comparator<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {
//
//			@Override
//			public int compare(
//					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> o1,
//					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> o2) {
//				
//				return o1.getKey().compareTo(o2.getKey());
//			}
//		});
    	
    	blockedPairs = proc.sort(blockedPairs, new Function<String, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(
					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> input) {
				return input.getKey();
			}
    		
    	});
    	
    	Map<String, Map<String, Set<String>>> tableLinksViaKey = new HashMap<>();
    	Map<Table, Map<Table, Map<String, Set<Integer>>>> allLinks = new HashMap<>();
    	
    	// print all the links
    	for(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> group : blockedPairs.get()) {
    		
    		// determine tables from group key
    		String[] ids = group.getKey().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		final Table t1 = web.getTables().get(id1);
    		final Table t2 = web.getTables().get(id2);
    		
    		// count key occurrences
    		HashMap<String, Set<Integer>> keyCounts1 = new HashMap<>();
    		HashMap<String, Set<Integer>> keyCounts2 = new HashMap<>();
    		
    		for(Collection<Integer> key : tableToCandidateKeys.get(id1)) {
    			String s = StringUtils.join(Q.project(key, new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in,  t1.getSchema().get(in).getHeader());
					}
				}), ",");
    			keyCounts1.put(s, new HashSet<Integer>());
    		}
    		for(Collection<Integer> key : tableToCandidateKeys.get(id2)) {
    			String s = StringUtils.join(Q.project(key, new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in, t2.getSchema().get(in).getHeader());
					}
				}), ",");
    			keyCounts2.put(s, new HashSet<Integer>());
    		}
    		
    		System.out.println(String.format("{#%d}%s <-> {#%d}%s", id1, t1.getPath(), id2, t2.getPath()));
    		System.out.println(String.format("%s <-> %s", tableToSchema.get(id1), tableToSchema.get(id2)));
    		
    		ResultSet<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> sorted = proc.sort(group.getRecords(), new Function<Integer, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Integer execute(
						Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> input) {
					return input.getFirst().getRow().getRowNumber();
				}
    			
    		});
    		
    		for(final Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> link : sorted.get()) {
    			
    			String key1 = StringUtils.join(Q.project(link.getFirst().getKey(), new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in,  t1.getSchema().get(in).getHeader());
					}
				}), ",");
    			String key2 = StringUtils.join(Q.project(link.getSecond().getKey(), new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in, t2.getSchema().get(in).getHeader());
					}
				}), ",");
    			
    			if(!keyCounts1.containsKey(key1)) {
    				keyCounts1.put(key1,new HashSet<Integer>());
    			}
    			if(!keyCounts2.containsKey(key2)) {
    				keyCounts2.put(key2,new HashSet<Integer>());
    			}
    			keyCounts1.get(key1).add(link.getFirst().getRow().getRowNumber());
    			keyCounts2.get(key2).add(link.getSecond().getRow().getRowNumber());
    			
    			System.out.println(String.format("{%s}->{%s} : [%s]", key1, key2, StringUtils.join(Q.project(link.getFirst().getKey(), new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return (String)link.getFirst().getRow().get(in);
					}
				}), ",")));
    			System.out.println(String.format("\t[%d] %s\t%s", link.getFirst().getRow().getRowNumber(), link.getFirst().getRow().format(20), link.getFirst().getRow().get(0)));
    			System.out.println(String.format("\t[%d] %s\t%s", link.getSecond().getRow().getRowNumber(), link.getSecond().getRow().format(20), link.getSecond().getRow().get(0)));
    			System.out.println();
    		}
    		
    		Map<String, Set<String>> keys = MapUtils.get(tableLinksViaKey, t1.getPath(), new HashMap<String, Set<String>>());
    		
    		// summarize key counts
    		System.out.println(t1.getPath());
    		int numKeysWithCors = 0;
    		for(String s : keyCounts1.keySet()) {
    			System.out.println(String.format("%s : %d", s, keyCounts1.get(s).size()));
    			
    			if(keyCounts1.get(s).size()>0) {
	    			Set<String> linkedTables = MapUtils.get(keys, s, new HashSet<String>());
	    			linkedTables.add(t2.getPath());
	    			
	    			MapUtils2.put(allLinks, t1, t2, keyCounts1);
	    			numKeysWithCors++;
    			}
    		}
    		System.out.println(t2.getPath());
    		for(String s : keyCounts2.keySet()) {
    			System.out.println(String.format("%s : %d", s, keyCounts2.get(s).size()));
    			
    			MapUtils2.put(allLinks, t2, t1, keyCounts2);
    		}
    		if(numKeysWithCors>1) {
    			System.out.println("* Multiple Matching Candidate Keys *");
    		}
    	}
    	
    	// summarize table linkage via keys
    	System.out.println("Table linkage via keys");
    	for(String t1 : tableLinksViaKey.keySet()) {
    		System.out.println("\t" + t1);
    		Map<String, Set<String>> keys = tableLinksViaKey.get(t1);
    		
    		// count number of keys linking to each table
    		Map<String, Set<String>> tables = new HashMap<>();
    		
    		for(String key : keys.keySet()) {
    			Set<String> linkedTables = keys.get(key);
    			System.out.println(String.format("\t\t%d\t%s\t%s", linkedTables.size(), key, StringUtils.join(linkedTables, ",")));
    			
    			for(String table : linkedTables){
    				Set<String> linkingKeys = MapUtils.get(tables, table, new HashSet<String>());
    				linkingKeys.add(key);
    			}
    		}
    		
    		for(String table : tables.keySet()) {
    			Set<String> linkingKeys = tables.get(table);
    			if(linkingKeys.size()>1) {
    				System.out.println(String.format("\t\t%d\t%s\t%s", linkingKeys.size(), table, StringUtils.join(linkingKeys, "||")));
    			}
    		}
    	}
    	
    	return allLinks;
	}
	
	private static class tableRowPair {
		public int table1;
		public int table2;
		public int row1;
		public int row2;
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + row1;
			result = prime * result + row2;
			result = prime * result + table1;
			result = prime * result + table2;
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			tableRowPair other = (tableRowPair) obj;
			if (row1 != other.row1)
				return false;
			if (row2 != other.row2)
				return false;
			if (table1 != other.table1)
				return false;
			if (table2 != other.table2)
				return false;
			return true;
		}
		
		
	}
	
	private void printValueLinkStatistics(ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs, 
			DataProcessingEngine proc, final int minKeysPerCorrespondence) {

		// group by table ids and keys to get the number of matching keys per correspondence
//		RecordKeyValueMapper<tableRowPair, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> mapToTablesAndRows = new RecordKeyValueMapper<tableRowPair, Group<String,Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>, Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> record,
//					DatasetIterator<Pair<tableRowPair, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> resultCollector) {
//				
//				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : record.getRecords().get()) {
//					
//					List<MatchableTableRow> rows = new ArrayList<>();
//					rows.add(cor.getFirst().getRow());
//					rows.add(cor.getSecond().getRow());
//					Collections.sort(rows, new MatchableTableRow.RowNumberComparator());
//					
//					MatchableTableRow row1 = rows.get(0);
//					MatchableTableRow row2 = rows.get(1);
//					
//					tableRowPair key = new tableRowPair();
//					key.table1 = row1.getTableId();
//					key.row1 = row1.getRowNumber();
//					key.table2 = row2.getTableId();
//					key.row2 = row2.getRowNumber();
//					
//					resultCollector.next(new Pair<tableRowPair, Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>(key, cor));
//					
//				}
//				
//			}
//		};
//		
//		ResultSet<Group<tableRowPair, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> keysByCorrespondences = proc.groupRecords(blockedPairs, mapToTablesAndRows);

		
//		Function<String, Group<tableRowPair, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> tableRowPairToSortingKeyMapper = new Function<String, Group<tableRowPair,Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(Group<tableRowPair, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> input) {
//				return String.format("%d/%d-%d/%d", input.getKey().table1, input.getKey().table2, input.getKey().row1, input.getKey().row2);
//			}
//		}; 
//		keysByCorrespondences = proc.sort(keysByCorrespondences, tableRowPairToSortingKeyMapper);
		
		
		RecordMapper<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>, Pair<String, Pair<Distribution<String>, Distribution<String>>>> recordLinksToDistributionsMapper = new RecordMapper<Group<String,Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>, Pair<String, Pair<Distribution<String>, Distribution<String>>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> record,
					DatasetIterator<Pair<String, Pair<Distribution<String>, Distribution<String>>>> resultCollector) {
				
				Distribution<String> keysPerCorrespondenceDistribution = new Distribution<>();
				Distribution<String> correspondencesPerKeyDistribution = new Distribution<>();
				
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : record.getRecords().get()) {
					
					MatchableTableRowWithKey row1 = cor.getFirst();
					MatchableTableRowWithKey row2 = cor.getSecond();
//					
//					List<MatchableTableRowWithKey> rows = Q.sort(Q.toList(cor.getFirst(), cor.getSecond()), new Comparator<MatchableTableRowWithKey>() {
//
//						@Override
//						public int compare(MatchableTableRowWithKey o1, MatchableTableRowWithKey o2) {
//							return Integer.compare(o1.getRow().getRowNumber(), o2.getRow().getRowNumber());
//						}
//					});
					
					String correspondence = String.format("%d/%d", row1.getRow().getRowNumber(), row2.getRow().getRowNumber());
					String key = String.format("{%s}<->{%s}", row1.getKey(), row2.getKey());

					// we have a new key for 'correspondence'
					keysPerCorrespondenceDistribution.add(correspondence);
					
					// we have a new correspondence for 'key'
					correspondencesPerKeyDistribution.add(key);
				}
				
				if(correspondencesPerKeyDistribution.getElements().size()>minKeysPerCorrespondence) {
					// only interesting if there is more than one matching key
					resultCollector.next(new Pair<String, Pair<Distribution<String>,Distribution<String>>>(record.getKey(), new Pair<Distribution<String>, Distribution<String>>(keysPerCorrespondenceDistribution, correspondencesPerKeyDistribution)));
				}
			}
		};
		ResultSet<Pair<String, Pair<Distribution<String>, Distribution<String>>>> distributions = proc.transform(blockedPairs, recordLinksToDistributionsMapper);
		
		
		
		// sort all the groups by (from table),(to table)
		distributions = proc.sort(distributions, new Function<String, Pair<String, Pair<Distribution<String>, Distribution<String>>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(
					Pair<String, Pair<Distribution<String>, Distribution<String>>> input) {
				return input.getFirst();
			}
    		
    	});
		
		System.out.println("*** Key/Correspondence Distributions for value-based links ***");
    	for(Pair<String, Pair<Distribution<String>, Distribution<String>>> value : distributions.get()) {
    		
    		// determine tables from group key
    		String[] ids = value.getFirst().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		final Table t1 = web.getTables().get(id1);
    		final Table t2 = web.getTables().get(id2);
    		
    		System.out.println(String.format("{#%d}%s/{#%d}%s", id1, t1.getPath(), id2, t2.getPath()));
    		System.out.println("Number of keys per correspondence");
    		System.out.println(value.getSecond().getFirst().format());
    		System.out.println("Number of correspondences per key");
    		System.out.println(value.getSecond().getSecond().format());
    	}
	}
	
	private ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> removeUntrustedValueLinks(
			ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs, 
			DataProcessingEngine proc,
			final HashMap<Integer, String> tableToSchema,
			final HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys) {
		
    	
    	// now filter out untrusted correspondences
		RecordMapper<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> removeUntrustedLinksMapper = new RecordMapper<Group<String,Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> record,
					DatasetIterator<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> resultCollector) {
				
				Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> correspondences = new LinkedList<>(record.getRecords().get()); 
				Integer t1Id = null;
				Integer t2Id = null;
				
				// determine distributions of correspondences per key / keys per correspondence
				Distribution<Collection<Integer>> correspondencesPerKeyDistribution1 = new Distribution<>();
				Distribution<Collection<Integer>> correspondencesPerKeyDistribution2 = new Distribution<>();
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : correspondences) {
					
					if(t1Id == null) {
						t1Id = cor.getFirst().getRow().getTableId();
					}
					if(t2Id == null) {
						t2Id = cor.getSecond().getRow().getTableId();
					}
					
					MatchableTableRowWithKey row1 = cor.getFirst();
					MatchableTableRowWithKey row2 = cor.getSecond();
					
					// we have a new correspondence for 'key'
					correspondencesPerKeyDistribution1.add(row1.getKey());
					correspondencesPerKeyDistribution2.add(row2.getKey());
				}
				
				// find the candidate keys that did not create any correspondences
				Collection<Collection<Integer>> t1MissingKeys = new LinkedList<>();
				for(Collection<Integer> candKey : tableToCandidateKeys.get(t1Id)) {
					if(correspondencesPerKeyDistribution1.getFrequency(candKey)==0) {
						t1MissingKeys.add(candKey);
					}
				}
				Collection<Collection<Integer>> t2MissingKeys = new LinkedList<>();
				for(Collection<Integer> candKey : tableToCandidateKeys.get(t2Id)) {
					if(correspondencesPerKeyDistribution2.getFrequency(candKey)==0) {
						t2MissingKeys.add(candKey);
					}
				}
				
				// try to find the values for that candidate key in the corresponding records (this time not only in the candidate keys, but in all attributes)
				Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> newCorrespondences = new LinkedList<>();
				// keep track of the checked rows, as we have duplicate correspondences
				final Set<String> checkedCors = new HashSet<>();
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : correspondences) {
					String corString = String.format("%s-%s", cor.getFirst().getRow().getRowNumber(), cor.getSecond().getRow().getRowNumber());
					if(!checkedCors.contains(corString)) {
						
						// check keys from table1
						for(Collection<Integer> candKey : t1MissingKeys) {
							
							// get the key values for candKey in table1
							LinkedList<String> t1Values = new LinkedList<>();
							for(Integer index : candKey) {
								Object value = cor.getFirst().getRow().get(index);
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
							MatchableTableRow row2 = cor.getSecond().getRow();
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
										new MatchableTableRowWithKey(cor.getFirst().getRow(), candKey), 
										new MatchableTableRowWithKey(cor.getSecond().getRow(), matchingValueIndices));
								newCorrespondences.add(newCor);
							}
						}
						
						// check keys from table2
						for(Collection<Integer> candKey : t2MissingKeys) {
							
							// get the key values for candKey in table2
							LinkedList<String> t2Values = new LinkedList<>();
							for(Integer index : candKey) {
								Object value = cor.getSecond().getRow().get(index);
								String stringValue = null;
								if(value==null) {
									stringValue = "NULL";
								} else {
									stringValue = value.toString();
								}
								t2Values.add(stringValue);
							}
							
							// try to find the values in table2
							Collection<Integer> matchingValueIndices = new HashSet<>();
							MatchableTableRow row1 = cor.getFirst().getRow();
							for(int i = 0; i < row1.getRowLength(); i++) {
								Object value = row1.get(i);
								String stringValue = null;
								if(value==null) {
									stringValue = "NULL";
								} else {
									stringValue = value.toString();
								}
								
								if(t2Values.contains(stringValue)) {
									matchingValueIndices.add(i);
									t2Values.remove(stringValue);
								}
							}
							
							// have all values been found?
							if(t2Values.size()==0) {
								// if yes, create a new correspondence
								Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> newCor = new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(
										new MatchableTableRowWithKey(cor.getFirst().getRow(), matchingValueIndices), 
										new MatchableTableRowWithKey(cor.getSecond().getRow(), candKey));
								newCorrespondences.add(newCor);
							}
						}
						
						checkedCors.add(corString);
					}
				}
				
				// add the new correspondences to the existing ones
				correspondences.addAll(newCorrespondences);
				
				// determine the frequency of candidate key combinations 
				SimilarityMatrix<WebTableKey> keyCombinations = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
				final HashMap<Collection<Integer>, Collection<Integer>> trustedKeyCombinations = new HashMap<>();
				
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : correspondences) {
					
					MatchableTableRowWithKey row1 = cor.getFirst();
					MatchableTableRowWithKey row2 = cor.getSecond();
					
					// we have a new key combination
					WebTableKey k1 = new WebTableKey(row1.getRow().getTableId(), new HashSet<>(row1.getKey()));
					WebTableKey k2 = new WebTableKey(row2.getRow().getTableId(), new HashSet<>(row2.getKey()));
					Double last = keyCombinations.get(k1,k2);
					if(last==null) {
						last = 0.0;
					}
					keyCombinations.set(k1, k2, last+1.0);
				}
				
				// determine the trusted combinations
				// if key1 is combined with more than one other key, only one can be correct
				// we simply trust the most frequent combination
				BestChoiceMatching bcm = new BestChoiceMatching();
				keyCombinations = bcm.match(keyCombinations);
				
				for(WebTableKey k1 : keyCombinations.getFirstDimension()) {
					for(WebTableKey k2 : keyCombinations.getMatches(k1)) {
						trustedKeyCombinations.put(k1.getColumnIndices(), k2.getColumnIndices());
					}
				}
				
				// only keep correspondences from trusted keys
				correspondences = Q.where(correspondences, new Func<Boolean, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>() {

					@Override
					public Boolean invoke(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor) {
						MatchableTableRowWithKey row1 = cor.getFirst();
						MatchableTableRowWithKey row2 = cor.getSecond();
						
						Set<Integer> k1 = new HashSet<>(row1.getKey());
						Set<Integer> k2 = new HashSet<>(row2.getKey());
						return trustedKeyCombinations.containsKey(k1)
								&& trustedKeyCombinations.get(k1).equals(k2);
					}
				});
				
				// determine distributions of correspondences per key / keys per correspondence
				final Distribution<String> keysPerCorrespondenceDistribution = new Distribution<>();
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : correspondences) {
					
					MatchableTableRowWithKey row1 = cor.getFirst();
					MatchableTableRowWithKey row2 = cor.getSecond();
					
					String correspondence = String.format("%d/%d", row1.getRow().getRowNumber(), row2.getRow().getRowNumber());

					// we have a new key for 'correspondence'
					keysPerCorrespondenceDistribution.add(correspondence);
				}
				

				
				
//				if(correspondencesPerKeyDistribution.getElements().size()>1) {
					// only interesting if there is more than one matching key
					
				correspondences = Q.where(correspondences, new Func<Boolean, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>(){

					@Override
					public Boolean invoke(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor) {
						MatchableTableRowWithKey row1 = cor.getFirst();
						MatchableTableRowWithKey row2 = cor.getSecond();
						
						String correspondence = String.format("%d/%d", row1.getRow().getRowNumber(), row2.getRow().getRowNumber());
						
						// only keep a record if it was created by all trusted keys
						return keysPerCorrespondenceDistribution.getFrequency(correspondence)==trustedKeyCombinations.size();
					}});
					
					if(correspondences.size()>0) {
						// if there are any links left, keep them
//						resultCollector.next(record);
						
						// but deduplicate first
						HashMap<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> existing = new HashMap<>();
						Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> oldRecords = correspondences;
						
						ResultSet<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> newRecords = new ResultSet<>();
						
						for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : oldRecords) {

							List<MatchableTableRowWithKey> rows = Q.toList(cor.getFirst(), cor.getSecond());
							Collections.sort(rows, new MatchableTableRowWithKey.RowNumberComparator());
							
							MatchableTableRowWithKey row1 = rows.get(0);
							MatchableTableRowWithKey row2 = rows.get(1);

							String correspondence = String.format("%d/%d", row1.getRow().getRowNumber(), row2.getRow().getRowNumber());
							
							if(existing.containsKey(correspondence)) {
								// add the key indices to the existing correspondence
								Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> existingCor = existing.get(correspondence);
								existingCor.getFirst().getKey().addAll(cor.getFirst().getKey());
								existingCor.getSecond().getKey().addAll(cor.getSecond().getKey());
							} else {
								existing.put(correspondence, 
										new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(
												new MatchableTableRowWithKey(cor.getFirst().getRow(), new HashSet<>(cor.getFirst().getKey())), 
												new MatchableTableRowWithKey(cor.getSecond().getRow(), new HashSet<>(cor.getSecond().getKey()))));
								newRecords.add(existing.get(correspondence));
							}
						}
//						
						resultCollector.next(new Group<String, Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>(record.getKey(), newRecords));
					}
					
//				}
			}
		};
		
		return proc.transform(blockedPairs, removeUntrustedLinksMapper);
	}

	private ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> verifyValueLinkTransitivity(
			ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs, 
			DataProcessingEngine proc,
			final HashMap<Integer, String> tableToSchema,
			final HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys) {
		
		Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> toRemove = new LinkedList<>();
		Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> toAdd = new LinkedList<>();
		
		Map<Integer, Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>> linksByTables = new HashMap<>();
		
		for(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> group : blockedPairs.get()) {
    		// determine the tables from the grouping key
    		String[] ids = group.getKey().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		Table t1 = web.getTables().get(id1);
    		Table t2 = web.getTables().get(id2);   	
    		
    		MapUtils2.put(linksByTables, id1, id2, group.getRecords().get());
		}
		
		
		// table -> table -> row -> { correspondences }
		Map<Integer, Map<Integer, Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>>> linksByRecord = new HashMap<>();
		// table -> row -> { correspondences }
		Map<Integer, Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>> linksFromTable = new HashMap<>();
		// for each table
		for(Integer t1 : linksByTables.keySet()) {
		//  check all record links to other tables by record
			Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> recordLinksFromTable = MapUtils.get(linksFromTable, t1, new HashMap<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>());
			
			for(Integer t2 : linksByTables.get(t1).keySet()) {
				
				Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> recordLinks = MapUtils2.get(linksByRecord, t1, t2);
				if(recordLinks==null) {
					recordLinks=new HashMap<>();
					MapUtils2.put(linksByRecord, t1, t2, recordLinks);
				}
				
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> link : linksByTables.get(t1).get(t2)) {
				
					// links between t1 and t2
					Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> links = recordLinks.get(link.getFirst().getRow().getRowNumber());
					if(links==null) {
						links = new LinkedList<>();
						recordLinks.put(link.getFirst().getRow().getRowNumber(), links);
					}
					links.add(link);
					
					// links from t1
					links = recordLinksFromTable.get(link.getFirst().getRow().getRowNumber());
					if(links==null) {
						links = new LinkedList<>();
						recordLinksFromTable.put(link.getFirst().getRow().getRowNumber(), links);
					}
					links.add(link);
				}
			}
		}
		
		for(Integer t1 : linksFromTable.keySet()) {
		//   if a record has correspondences in two other tables, check if the transitive connection exists
			
			Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> tableLinks = linksFromTable.get(t1);
			
			for(Integer row : tableLinks.keySet()) {
				Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> links = tableLinks.get(row);
				
				if(links.size()>1) {
					
					List<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> linkList = new ArrayList<>(links);
					
					for(int i = 0; i < linkList.size(); i++) {
						Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> corA = linkList.get(i);
						int tA = corA.getSecond().getRow().getTableId();
						int rA = corA.getSecond().getRow().getRowNumber();
						
						Map<Integer, Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>> linksA = linksByRecord.get(tA);
						
						for(int j = i+1; j < linkList.size(); j++) {
							Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> corB = linkList.get(j);
							int tB = corB.getSecond().getRow().getTableId();
							int rB = corB.getSecond().getRow().getRowNumber();
							
							if(tA!=tB) {
								boolean found = false;
								
								Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> linksAB = linksA.get(tB);
								
								if(linksAB!=null) {
									Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> recordLinks = linksAB.get(rA);
									
									if(recordLinks!=null) {
										for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> link : recordLinks) {
											if(link.getSecond().getRow().getRowNumber()==rB) {
												found = true;
												break;
											}
										}
									}
								}
								
								if(!found) {
									
									// check if there should be a correspondence
									Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> newCor = null;
									boolean created = false;
									for(Collection<Integer> candidateKey : tableToCandidateKeys.get(tA)) {
										newCor = matches(candidateKey, corA.getSecond().getRow(), corB.getSecond().getRow());
										if(newCor!=null) {
											toAdd.add(newCor);
											toAdd.add(new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(
													new MatchableTableRowWithKey(newCor.getSecond().getRow(), newCor.getSecond().getKey()), 
													new MatchableTableRowWithKey(newCor.getFirst().getRow(), newCor.getSecond().getKey())));
											System.out.println(String.format("&&& Adding {#%d}[#%d]{%s}<->{#%d}[#%d]{%s}<->{#%d}[#%d]{%s} (via transitivity)", tA, rA, newCor.getFirst().getKey(), t1, row, corA.getFirst().getKey(), tB, rB, newCor.getSecond().getKey()));
											System.out.println(String.format("\t{%d}[%d]\t%s", tA, rA, newCor.getFirst().getRow().format(20)));
											System.out.println(String.format("\t{%d}[%d]\t%s", t1, row, corA.getFirst().getRow().format(20)));
											System.out.println(String.format("\t{%d}[%d]\t%s", tB, rB, newCor.getSecond().getRow().format(20)));
											created = true;
										}
									}
									
									
									if(!created) {
										//     if not, remove both record links, as they are incorrect (the transitive connection must have been checked before) 
										toRemove.add(corA);
										toRemove.add(corB);
										
										System.out.println(String.format("&&& Removing {#%d}[#%d]{%s}<->{#%d}[#%d]{%s}<->{#%d}[#%d]{%s} (violated transitivity)", tA, rA, corA.getSecond().getKey(), t1, row, corA.getFirst().getKey(), tB, rB, corB.getSecond().getKey()));
										System.out.println(String.format("\t{%d}[%d]\t%s", tA, rA, corA.getSecond().getRow().format(20)));
										System.out.println(String.format("\t{%d}[%d]\t%s", t1, row, corA.getFirst().getRow().format(20)));
										System.out.println(String.format("\t{%d}[%d]\t%s", tB, rB, corB.getSecond().getRow().format(20)));
									}
								}
							}
						}
					}
					
				}
			}
		}
			
		Iterator<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> it = blockedPairs.get().iterator();
		
		while(it.hasNext()) {
			Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> group = it.next();
			
    		String[] ids = group.getKey().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		
    		Iterator<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> toAddIt = toAdd.iterator();
    		while(toAddIt.hasNext()) {
    			Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor = toAddIt.next();
    			if(cor.getFirst().getRow().getTableId()==id1 && cor.getSecond().getRow().getTableId()==id2) {
    				group.getRecords().add(cor);
    				toAddIt.remove();
    			}
    		}
			
			group.getRecords().get().removeAll(toRemove);
			if(group.getRecords().size()==0) {
				it.remove();
			}
		}
		
		return blockedPairs;
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
	
	public void printInstanceCorrespondenceStatistics(ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences) {

//		RecordKeyValueMapper<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>> groupByTableCombination = new RecordKeyValueMapper<String, Correspondence<MatchableTableRow,MatchableTableColumn>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableColumn> record,
//					DatasetIterator<Pair<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {
//				
//				String key = record.getFirstRecord().getTableId() + "/" + record.getSecondRecord().getTableId();
//				
//				resultCollector.next(new Pair<String, Correspondence<MatchableTableRow,MatchableTableColumn>>(key, record));
//				
//			}
//		};
//		
//		ResultSet<Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> groupedByTable = proc.groupRecords(correspondences, groupByTableCombination);
//		
//		RecordMapper<Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>>, Pair<String, Pair<Distribution<String>, Distribution<String>>>> recordLinksToDistributionsMapper = new RecordMapper<Group<String,Correspondence<MatchableTableRow, MatchableTableColumn>>, Pair<String, Pair<Distribution<String>, Distribution<String>>>>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>> record,
//					DatasetIterator<Pair<String, Pair<Distribution<String>, Distribution<String>>>> resultCollector) {
//				
//				Distribution<String> keysPerCorrespondenceDistribution = new Distribution<>();
//				Distribution<String> correspondencesPerKeyDistribution = new Distribution<>();
//				
//				for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : record.getRecords().get()) {
//					
//					MatchableTableRow row1 = cor.getFirstRecord();
//					MatchableTableRow row2 = cor.getSecondRecord();;
//					
//					String correspondence = String.format("%d/%d", row1.getRowNumber(), row2.getRowNumber());
//					String key = String.format("{%s}<->{%s}", row1.getKey(), row2.getKey());
//
//					// we have a new key for 'correspondence'
//					keysPerCorrespondenceDistribution.add(correspondence);
//					
//					// we have a new correspondence for 'key'
//					correspondencesPerKeyDistribution.add(key);
//				}
//				
//				if(correspondencesPerKeyDistribution.getElements().size()>minKeysPerCorrespondence) {
//					// only interesting if there is more than one matching key
//					resultCollector.next(new Pair<String, Pair<Distribution<String>,Distribution<String>>>(record.getKey(), new Pair<Distribution<String>, Distribution<String>>(keysPerCorrespondenceDistribution, correspondencesPerKeyDistribution)));
//				}
//			}
//		};
//		ResultSet<Pair<String, Pair<Distribution<String>, Distribution<String>>>> distributions = proc.transform(blockedPairs, recordLinksToDistributionsMapper);
//		
//		
//		
//		// sort all the groups by (from table),(to table)
//		distributions = proc.sort(distributions, new Function<String, Pair<String, Pair<Distribution<String>, Distribution<String>>>>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(
//					Pair<String, Pair<Distribution<String>, Distribution<String>>> input) {
//				return input.getFirst();
//			}
//    		
//    	});
//		
//		System.out.println("*** Key/Correspondence Distributions for value-based links ***");
//    	for(Pair<String, Pair<Distribution<String>, Distribution<String>>> value : distributions.get()) {
//    		
//    		// determine tables from group key
//    		String[] ids = value.getFirst().split("/");
//    		int id1 = Integer.parseInt(ids[0]);
//    		int id2 = Integer.parseInt(ids[1]);
//    		final Table t1 = web.getTables().get(id1);
//    		final Table t2 = web.getTables().get(id2);
//    		
//    		System.out.println(String.format("{#%d}%s/{#%d}%s", id1, t1.getPath(), id2, t2.getPath()));
//    		System.out.println("Number of keys per correspondence");
//    		System.out.println(value.getSecond().getFirst().format());
//    		System.out.println("Number of correspondences per key");
//    		System.out.println(value.getSecond().getSecond().format());
//    	}
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
	
	private void printKeyAndSchemaCorrespondencesCorrespondences(Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences) {
				
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
		
		Map<String, Collection<Correspondence<MatchableTableKey, MatchableTableColumn>>> grouped = Q.group(sorted, new Func<String, Correspondence<MatchableTableKey, MatchableTableColumn>>() {

			@Override
			public String invoke(Correspondence<MatchableTableKey, MatchableTableColumn> in) {
				return in.getFirstRecord().getTableId() + "<->" + in.getSecondRecord().getTableId();
			}});
		
		for(String key : grouped.keySet()) {
			System.out.println(key);
			
			Correspondence<MatchableTableKey, MatchableTableColumn> firstCor = Q.firstOrDefault(grouped.get(key));
			for(Correspondence<MatchableTableColumn, MatchableTableKey> cause : firstCor.getCausalCorrespondences().get()) {
				System.out.println(String.format("\t%s<->%s", cause.getFirstRecord(), cause.getSecondRecord()));
			}
			
			for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : grouped.get(key)) {
				System.out.println(String.format("{#%d}{%s} <-> {#%d}{%s}", 
						cor.getFirstRecord().getTableId(), 
						Q.project(cor.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()),
						cor.getSecondRecord().getTableId(), 
						Q.project(cor.getSecondRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection())));
			}
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
}
