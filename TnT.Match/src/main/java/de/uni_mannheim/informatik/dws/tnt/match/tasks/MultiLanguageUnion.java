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
import de.uni_mannheim.informatik.dws.t2k.utils.data.graph.Partitioning;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.CSVTableWriter;
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
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTable;
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
import de.uni_mannheim.informatik.dws.tnt.match.rules.MultiLanguageUnionTables.MultiLanguageUnionSchemaToTableAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.MultiLanguageUnionTables.MultiLanguageUnionTableBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.MultiLanguageUnionTables.MultiLanguageUnionTableSchemaBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.BestChoiceGroupingMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.ColumnIndexAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceByKeyToSchemaAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceToSchemaAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceTransitivityAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.KeyPropagationAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaSynonymAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaToInstanceAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaToKeyKeyAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaTransitivityAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.TrustedKeyAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.ColumnIndexSchemaBlocker;
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
public class MultiLanguageUnion extends TnTTask {

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
		MultiLanguageUnion tju = new MultiLanguageUnion();

		if (tju.parseCommandLine(MultiLanguageUnion.class, args)) {

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
    	
    	ClusteringPerformance schemaPerformance = null;
    	N2NGoldStandard schemaMapping = null;
    	
    	
    	//TODO this step should run only on tables which likely have the same schema with different column headers (different languages)
    	// so we need to define conditions and blocking keys that ensure this property
    	// 1) same position in DOM tree (use table index as substitute)
    	// 2) same number of columns (including those generated from the URI)
    	// 3) no/only coincidental overlap in column headers (TODO define measure)
    	// -- look at the co-occurrence distribution of column headers among all selected tables)
    	// -- if we have multiple languages there should be no headers to co-occur for too many different languages..
    	
    	// find multi-language-union tables and create blocking keys
    	// forward all non-multi-language-union tables directly to the output directory
    	
    	ColumnIndexAggregator cia = new ColumnIndexAggregator();
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> columnIndexSchema = null;
    	SimilarityMatrix<MatchableTableColumn> columnIndexMatrix = null;
    	
    	MultiLanguageUnionTableBlocker tableBlocker = new MultiLanguageUnionTableBlocker();
    	ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> blockedTables = tableBlocker.runBlocking(web.getTableRecords(), true, null, proc);
    	
    	blockedTables = proc.filter(blockedTables, new Function<Boolean, Correspondence<MatchableTable, MatchableTableColumn>>() {

			@Override
			public Boolean execute(Correspondence<MatchableTable, MatchableTableColumn> input) {
				return input.getFirstRecord().getTableId()==2 && input.getSecondRecord().getTableId()==23;
			}
		});
    	
    	// table blocking so far is insufficient, we need an attribute statistics-based schema matcher to check if the attributes are compatible
    	MultiLanguageUnionTableSchemaBlocker schemaBlocker = new MultiLanguageUnionTableSchemaBlocker();
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTable>> blockedSchema = schemaBlocker.runBlocking(web.getRecords(), true, blockedTables, proc);
    	blockedSchema.deduplicate();
    	
//    	blockedSchema = proc.filter(blockedSchema, new Function<Boolean, Correspondence<MatchableTableColumn, MatchableTable>>() {
//
//			@Override
//			public Boolean execute(Correspondence<MatchableTableColumn, MatchableTable> input) {
//				return input.getFirstRecord().getTableId()==22 && input.getSecondRecord().getTableId()==23;
//			}
//		});
    	
    	// add 1:1 mapping per table combination as GroupingMatcher here
    	BestChoiceGroupingMatcher<List<Integer>,MatchableTableColumn, MatchableTable> bestChoice = new BestChoiceGroupingMatcher<>(new Function<List<Integer>, Correspondence<MatchableTableColumn, MatchableTable>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public List<Integer> execute(Correspondence<MatchableTableColumn, MatchableTable> input) {
				return Q.toList(input.getFirstRecord().getTableId(), input.getSecondRecord().getTableId());
			}
		});
    	blockedSchema = bestChoice.aggregate(blockedSchema, proc);
    	
    	MultiLanguageUnionSchemaToTableAggregator schemaToTableAggregator = new MultiLanguageUnionSchemaToTableAggregator();
    	ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> schemaBasedTables = schemaToTableAggregator.aggregate(blockedSchema, proc);
    	SimilarityMatrix<MatchableTable> tableSim = SimilarityMatrix.fromCorrespondences(schemaBasedTables.get(), new SparseSimilarityMatrixFactory());
//    	System.out.println(tableSim.getOutput());
    	for(Correspondence<MatchableTable, MatchableTableColumn> cor : schemaBasedTables.get()) {
    		System.out.println(String.format("%d\t%d\t%.2f", cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId(), (double)cor.getSimilarityScore() / (double)(cor.getFirstRecord().getSchema().length-SpecialColumns.ALL.size())));
    	}
    	
    	
    	/*********************************************** 
    	 * the value-based approach produces all possible, correct links if all tables have the same set of attributes
    	 * missing correspondences are attributes with no overlapping values/headers between the different tables
    	 ***********************************************/
    	ValueBasedSchemaBlocker valueBasedSchemaBlocker = new ValueBasedSchemaBlocker();
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> valueBasedSchema = valueBasedSchemaBlocker.runBlocking(web.getRecords(), true, null, proc);
    	schemaMapping = N2NGoldStandard.createFromCorrespondences(valueBasedSchema.get());
    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	System.out.println("v2_schema_graph_value_based.net");
    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), false));
    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_value_based.net"), web.getSchema(), valueBasedSchema);
    	
    	columnIndexSchema = cia.aggregate(valueBasedSchema, proc);
    	columnIndexMatrix = SimilarityMatrix.fromCorrespondences(columnIndexSchema.get(), new SparseSimilarityMatrixFactory());
    	System.out.println(columnIndexMatrix.getOutput());
    	
    	SchemaSynonymBlocker generateSynonyms0 = new SchemaSynonymBlocker();
    	ResultSet<Set<String>> synonyms0 = generateSynonyms0.runBlocking(web.getSchema(), true, valueBasedSchema, proc);
    	System.out.println("Synonyms");
    	for(Set<String> clu : synonyms0.get()) {
    		System.out.println("\t" + StringUtils.join(clu, ","));
    	}
    	dh.extendWithSynonyms(synonyms0.get());
    	SynonymBasedSchemaBlocker synonymBlocker0 = new SynonymBasedSchemaBlocker(synonyms0);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> valueSchemaCorrespondencesFromSynonyms = synonymBlocker0.runBlocking(web.getSchema(), true, null, proc);    	
    	valueSchemaCorrespondencesFromSynonyms.deduplicate();
    	System.out.println(String.format("Synonyms: %d schema correspondences", valueSchemaCorrespondencesFromSynonyms.size()));
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : valueSchemaCorrespondencesFromSynonyms.get()) {
    		valueBasedSchema.add(cor);
    	}
    	valueBasedSchema.deduplicate();
    	System.out.println(String.format("== Total: %d schema correspondences", valueBasedSchema.size()));
    	
    	// remove special columns from schema correspondences (synonymBlocker is a blocker and uses all columns, so new correspondences for special columns can be created) 
    	SpecialColumnsSchemaFilter specialColumnsFilter0 = new SpecialColumnsSchemaFilter();
    	valueBasedSchema = specialColumnsFilter0.run(valueBasedSchema, proc);
    	System.out.println(String.format("-> Special Columns: %d schema correspondences", valueBasedSchema.size()));
    	
    	schemaMapping = N2NGoldStandard.createFromCorrespondences(valueBasedSchema.get());
    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	System.out.println("v2_schema_graph_value_based_synonyms.nett");
    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), false));
    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_value_based_synonyms.net"), web.getSchema(), valueBasedSchema);
    	
    	
    	// infer the missing correspondences based on the column indices
    	columnIndexSchema = cia.aggregate(valueBasedSchema, proc);
    	columnIndexMatrix = SimilarityMatrix.fromCorrespondences(columnIndexSchema.get(), new SparseSimilarityMatrixFactory());
    	System.out.println(columnIndexMatrix.getOutput());
    	columnIndexMatrix.normalize();
    	
    	//TODO add a verification step here to check if columns can really be matched via their indices
    	
    	BestChoiceMatching bcm = new BestChoiceMatching();
    	columnIndexMatrix = bcm.match(columnIndexMatrix);
    	for(MatchableTableColumn c1 : columnIndexMatrix.getFirstDimension()) {
    		for(MatchableTableColumn c2 : columnIndexMatrix.getMatches(c1)) {
    			if(columnIndexMatrix.get(c1, c2)==1.0) {
    				columnIndexMatrix.set(c1, c2, null);
    			}
    		}
    	}
    	// get correspondences from the normalised matrix
    	
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> columnIndexCorrespondences = new ResultSet<>(columnIndexMatrix.<MatchableTableRow>toCorrespondences());
    	ColumnIndexSchemaBlocker cisb = new ColumnIndexSchemaBlocker();
    	columnIndexCorrespondences = cisb.runBlocking(web.getSchema(), true, columnIndexCorrespondences, proc);
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : columnIndexCorrespondences.get()) {
    		valueBasedSchema.add(cor);
    	}
    	
    	schemaMapping = N2NGoldStandard.createFromCorrespondences(valueBasedSchema.get());
    	schemaPerformance = schemaGS.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	System.out.println("v2_schema_graph_value_based_columnIndex.nett");
    	System.out.println(schemaGS.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), false));
    	writeSchemaCorrespondenceGraph(new File(resultsLocationFile, "v2_schema_graph_value_based_columnIndex.net"), web.getSchema(), valueBasedSchema);
    	
    	long end = System.currentTimeMillis();
    	
    	System.out.println(String.format("Matching finished after %s", DurationFormatUtils.formatDurationHMS(end-start)));
    	
    	start = System.currentTimeMillis();
    	
    	// merge the tables
    	TableReconstructor tr = new TableReconstructor();
    	Table reconstructed = Q.firstOrDefault(tr.reconstruct(Q.max(web.getTables().keySet()), web.getRecords(), web.getSchema(), web.getCandidateKeys(), valueBasedSchema));
    	CSVTableWriter csvw = new CSVTableWriter();
    	csvw.write(reconstructed, new File(resultsLocationFile, reconstructed.getPath()));
    	
    	end = System.currentTimeMillis();
    	
    	System.out.println(String.format("Reconstruction finished after %s", DurationFormatUtils.formatDurationHMS(end-start)));
    	
    	System.out.println("done");
    	
    	
    	
    	
    	
    	
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
		
		Partitioning<MatchableTableColumn> columnIndexPartitioning = new Partitioning<>(g);
		for(MatchableTableColumn c : allAttributes.get()) {
			if(!SpecialColumns.isSpecialColumn(c)) {
				columnIndexPartitioning.setPartition(c, c.getColumnIndex());
			}
		}
		columnIndexPartitioning.writePajekFormat(new File(f.getAbsolutePath() + "_columnindex_partition.clu"));
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
