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
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;

import com.beust.jcommander.Parameter;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Pair;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.JsonTableWriter;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.MappingFormatter;
import de.uni_mannheim.informatik.dws.tnt.match.TableReconstructor;
import de.uni_mannheim.informatik.dws.tnt.match.TableReport;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NEvaluator;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandardCreator;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.DeterminantMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.rules.ValueBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceByDeterminantToSchemaAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceByKeyToSchemaAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaTransitivityAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.TrustedKeyAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyMappedCorrespondenceFilter;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlockerImproved2;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.SynonymBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.graph.ResolveConflictsByEdgeBetweenness;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.ContextColumnsSchemaFilter;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.DisjointHeaderSchemaMatchingRule;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.SpecialColumnsSchemaFilter;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.EntityLabelBasedMatching;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.LabelBasedTableMatching;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.ValueBasedTableMatching;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.matching.MatchingTask;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Performance;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;
import de.uni_mannheim.informatik.wdi.parallel.ParallelMatchingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableJoinUnion extends TnTTask {

	public static enum MatcherType {
		Trivial,
		NonTrivialPartial,
		NonTrivialFull,
		CandidateKey,
		Label,
		Entity
	}
	
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
	
	@Parameter(names = "-matcher")
	private MatcherType matcher = MatcherType.NonTrivialFull;
	
	public static void main(String[] args) throws Exception {
		TableJoinUnion tju = new TableJoinUnion();

		if (tju.parseCommandLine(TableJoinUnion.class, args)) {

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
//	private File csvResultsLocationFile;
	private File evaluationLocation;
	private N2NGoldStandard unionGs;
	
	public void initialise() throws Exception {
		printHeadline("Table Join Union");
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, true, false, serialise);
		web.removeHorizontallyStackedTables();
		
		
		// prepare output directories
    	resultsLocationFile = new File(new File(resultLocation), "join_union");
    	setOutputDirectory(resultsLocationFile);
    	resultsLocationFile.mkdirs();
    	
//    	csvResultsLocationFile = new File(new File(resultLocation), "join_union_csv");
//    	csvResultsLocationFile.mkdirs();
    	
    	evaluationLocation = new File(new File(resultLocation), "evaluation");
    	evaluationLocation.mkdirs();
    	
    	// prepare gold standard
    	N2NGoldStandardCreator gsc = new N2NGoldStandardCreator();
    	gsc.createFromMappedUnionTables(web.getTables().values(), evaluationLocation);
    	
    	File gsFile = new File(evaluationLocation, "union_goldstandard.tsv");
    	if(gsFile.exists()) {
    		unionGs = new N2NGoldStandard();
    		unionGs.loadFromTSV(gsFile);
    	}
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
    		
//    		for(int i = 0; i < t.getColumns().size(); i++) {
//    			TableColumn c1 = t.getSchema().get(i);
//    			
//    			if(!c1.getHeader().equals("null") && !c1.getHeader().isEmpty()) {
//    				Set<String> disjoint = MapUtils.get(disjointHeaders, c1.getHeader(), new HashSet<String>());
//    				
//    				Distribution<String> headerDist = Distribution.fromCollection(t.getColumns(), new TableColumn.ColumnHeaderProjection());
//    				
//	    			for(int j = 0; j < t.getColumns().size(); j++) {
//	    				TableColumn c2 = t.getSchema().get(j);
//	    				if(i!=j && !c2.getHeader().equals("null") && !c2.getHeader().isEmpty() && !c1.getHeader().equals(c2.getHeader())) {
//	    					disjoint.add(c2.getHeader());
//	    				}
//	    			}
//    			}
//    		}
    	}
//    	stat.print();

    	DisjointHeaders dh = DisjointHeaders.fromTables(web.getTables().values());
    	disjointHeaders = dh.getAllDisjointHeaders();
    	
    	// create sets of disjoint headers (which cannot have a schema correspondence as they appear in the same table)
    	System.out.println("Disjoint Headers:");
    	for(String header : disjointHeaders.keySet()) {
    		Set<String> disjoint = disjointHeaders.get(header);
    		System.out.println(String.format("\t%s\t%s", header, StringUtils.join(disjoint, ",")));
    	}

    	writeDisjointHeaders(dh);
    	
    	/***********************************************
    	 *********************************************** 
    	 * Re-Designed Matching Steps
    	 ***********************************************
    	 ***********************************************/
    	
    	long start = System.currentTimeMillis();
    	
    	System.out.println(String.format("%d records", web.getRecords().size()));
//    	DisjointHeaders dh = new DisjointHeaders(disjointHeaders);
    	
    	// print table ids
    	for(Integer id : Q.sort(web.getTables().keySet())) {
    		System.out.println(String.format("#%d\t%s", id, web.getTables().get(id).getPath()));
    	}

    	MappingFormatter mf = new MappingFormatter();
    	
//    	/*********************************************** 
//    	 * Initial record links
//    	 ***********************************************/
//    	
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = null;
//    	
//    	ValueBasedSchemaBlocker valueBasedSchemaBlocker = new ValueBasedSchemaBlocker(false,0.0);
//    	log(String.format("Value-based Schema Matching with %d records for %d schema elements", web.getRecords().size(), web.getSchema().size()));
//    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = valueBasedSchemaBlocker.runBlocking(web.getRecords(), true, null, proc);  
//
//    	/*********************************************** 
//    	 * match keys
//    	 ***********************************************/
//    	DeterminantMatcher determinantMatcher = new DeterminantMatcher();
//    	log(String.format("Determinant Matching with %d schema correspondences", schemaCorrespondences.size()));
//		ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = determinantMatcher.match(web, schemaCorrespondences, proc);
//		
//    	/*********************************************** 
//    	 * create new record links based on matching keys
//    	 ***********************************************/
//    	
//    	MatchingKeyRecordBlockerImproved2 matchingKeyBlocker = new MatchingKeyRecordBlockerImproved2();
//    	log(String.format("Determinant-based record linking with %d determinant matches", keyCorrespondences.size()));
//    	logDeterminants(keyCorrespondences);
//    	keyInstanceCorrespondences = matchingKeyBlocker.runBlocking(web.getRecords(), true, keyCorrespondences, proc);
//
//////    	TrustedKeyAggregator trustedKey = new TrustedKeyAggregator(1);
//////    	System.out.println(String.format("[TrustedKey] before: %d record links", keyInstanceCorrespondences.size()));
////    	TrustedKeyAggregator trustedKey = new TrustedKeyAggregator(2);
////    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> trustedKeyInstanceCorrespondences = trustedKey.aggregate(keyInstanceCorrespondences, proc);
//////    	System.out.println(String.format("[TrustedKey] after: %d record links", trustedKeyInstanceCorrespondences.size()));
////    	
////    	if(trustedKeyInstanceCorrespondences.size()==0){
////    		//TODO check: does trusted key with 1 make any difference?
////    		trustedKey = new TrustedKeyAggregator(1);
////        	keyInstanceCorrespondences = trustedKey.aggregate(keyInstanceCorrespondences, proc);	
////    	} else{
////    		keyInstanceCorrespondences = trustedKeyInstanceCorrespondences;
////    	}
//    	
//    	/*********************************************** 
//    	 * vote for schema correspondences
//    	 ***********************************************/
//    	
////    	InstanceByKeyToSchemaAggregator instanceVoteForSchema = new InstanceByKeyToSchemaAggregator(2, false, false, false);
//    	InstanceByDeterminantToSchemaAggregator instanceVoteForSchema = new InstanceByDeterminantToSchemaAggregator(0, false, false, false, 1.0);
//    	log(String.format("Voting for schema correspondences with %d record links", keyInstanceCorrespondences.size()));
//    	if(keyInstanceCorrespondences.size()<=20) {
//    		logRecordLinks(keyInstanceCorrespondences);
//    	}
////    	schemaCorrespondences  = instanceVoteForSchema.aggregate(keyInstanceCorrespondences, proc);
//    	schemaCorrespondences  = instanceVoteForSchema.aggregate(keyInstanceCorrespondences, schemaCorrespondences, proc);
//        	
//		DisjointHeaderSchemaMatchingRule disjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
//		log(String.format("Disjoint Header Rule for %d schema correspondences", schemaCorrespondences.size()));
//		schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
//    	
//    	/*********************************************** 
//    	 * refine schema correspondences
//    	 ***********************************************/
//		// only create correspondences between columns with the same header
//		// do not create synonyms from schema correspondences here (will be done later when we apply transitivity)
//		Set<Set<String>> s = new HashSet<Set<String>>();
//		for(MatchableTableColumn c : web.getSchema().get()) {
//			if(!c.getHeader().equals("null")) {
//				s.add(Q.toSet(c.getHeader()));
//			}
//		}
//		
//		ResultSet<Set<String>> synonyms = new ResultSet<>(s);
//		SynonymBasedSchemaBlocker synonymBlocker = new SynonymBasedSchemaBlocker(synonyms);
//		log(String.format("Synonym-based schema matching for %d schema elements", web.getSchema().size()));
//    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> synonymCorrespondences = synonymBlocker.runBlocking(web.getSchema(), true, null, proc);
//		log(String.format("Merged %d synonym-based and %d voting-based schema correspondences", synonymCorrespondences.size(), schemaCorrespondences.size()));
//    	schemaCorrespondences = proc.append(schemaCorrespondences, synonymCorrespondences);
//
////    	
//    	// remove special columns from schema correspondences (synonymBlocker is a blocker and uses all columns, so new correspondences for special columns can be created) 
//    	SpecialColumnsSchemaFilter specialColumnsFilter = new SpecialColumnsSchemaFilter();
//    	log("Special Columns Filter");
//    	schemaCorrespondences = specialColumnsFilter.run(schemaCorrespondences, proc);
//
//		
//    	/*********************************************** 
//    	 * graph-based optimisation
//    	 ***********************************************/
//    	ResolveConflictsByEdgeBetweenness rb = new ResolveConflictsByEdgeBetweenness(false);
//    	log(String.format("Graph-based conflict detection for %d schema correspondences", schemaCorrespondences.size()));
//		schemaCorrespondences = rb.match(schemaCorrespondences, proc, new DisjointHeaders(disjointHeaders));
    	
    	TableMatchingTask matcher = null;
    	
    	switch(this.matcher) {
		case CandidateKey:
			matcher = new ValueBasedTableMatching(false, true, true, true, false, false, false, 1.0, true,0,0);
			break;
		case Label:
			matcher = new LabelBasedTableMatching(false, false, false, false, false, false);
			break;
		case NonTrivialFull:
			matcher = new ValueBasedTableMatching(false, true, true, true, false, false, true, 1.0, true, 0, 0);
			break;
		case NonTrivialPartial:
			matcher = new ValueBasedTableMatching(false, true, true, true, false, false, true, 1.0, false,0,0);
			break;
		case Trivial:
			matcher = new ValueBasedTableMatching(false, false, true, true, false, false, false, 1.0, true,0,0);
			break;
		case Entity:
			matcher = new EntityLabelBasedMatching(false, false, true, true, false, false, 1.0,0);
			break;
		default:
			break;
    		
    	}
    	
//    	ValueBasedTableMatching matcher = new ValueBasedTableMatching(false, true, true, true, false, false, true, 1.0, true, 0, 0);
    	
    	matcher.setWebLocation(webLocation);
    	matcher.setResultLocation(resultLocation);
    	matcher.setWebTables(web);
    	matcher.setMatchingEngine(matchingEngine);
    	matcher.setDataProcessingEngine(proc);
    	matcher.setDisjointHeaders(disjointHeaders);
		
    	matcher.initialise();
    	matcher.match();
    	
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = matcher.getSchemaCorrespondences();
    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = matcher.getKeyCorrespondences();
    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = matcher.getKeyInstanceCorrespondences();
    	
    	// write correspondences
    	N2NGoldStandardCreator gsc = new N2NGoldStandardCreator();
    	gsc.writeInterUnionMapping(new File(evaluationLocation, "inter_union_mapping_generated.tsv"), schemaCorrespondences, web);

    	// apply transitivity
    	SchemaTransitivityAggregator transitivity = new SchemaTransitivityAggregator();
    	log(String.format("Transitivity for %d schema correspondences", schemaCorrespondences.size()));
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> transitiveCorrespondences = transitivity.aggregate(schemaCorrespondences, proc);
    	schemaCorrespondences = proc.append(schemaCorrespondences, transitiveCorrespondences);
    	mf.writeSchemaCorrespondenceGraph(new File(resultsLocationFile.getParentFile(), "schema_graph_transitivity.net"), web.getSchema(), schemaCorrespondences, unionGs);
    	schemaCorrespondences.deduplicate();
    	
    	long end = System.currentTimeMillis();
    	long matchingDuration = end-start;
    	log(String.format("Matching finished after %s", DurationFormatUtils.formatDurationHMS(matchingDuration)));
    	
    	// write correspondences including transitivity
    	matchingEngine.writeCorrespondences(schemaCorrespondences.get(), new File(resultsLocationFile.getParentFile(), "inter_union_correspondences.csv"));
    	
    	if(unionGs!=null) {
    		ContextColumnsSchemaFilter contextFilter = new ContextColumnsSchemaFilter();
    		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> evalCors = contextFilter.run(schemaCorrespondences, proc);
//	    	N2NEvaluator eval = new N2NEvaluator();
//	    	schemaCorrespondences.deduplicate();
	    	Performance perf = unionGs.evaluateCorrespondencePerformance(evalCors.get(), false);
//	    	ClusteringPerformance perf = eval.evaluateSchemaCorrespondences(schemaCorrespondences, web, proc, unionGs);
	    	System.out.println(String.format("Precision: %.4f\nRecall: %.4f\nF1: %.4f", perf.getPrecision(), perf.getRecall(), perf.getF1()));
    	}
    	
    	// statistics per correspondence cluster
    	Pair<Integer, Integer> clusterCounts = writeCorrespondenceStatistics(schemaCorrespondences);
    	int numberOfAttributeClusters = clusterCounts.getFirst(); 
    	int numberOfSynonymClusters = clusterCounts.getSecond();
    	
    	/*********************************************** 
    	 * merge the tables
    	 ***********************************************/
    	start = System.currentTimeMillis();
//    	int nextTableId = Q.max(web.getTables().keySet()) + 1;
    	int nextTableId = 0;
    	TableReconstructor tr = new TableReconstructor(web);
    	JsonTableWriter jsonw = new JsonTableWriter();
    	
    	// filter out correspondences between tables where no key is mapped and re-construct multiple tables
    	KeyMappedCorrespondenceFilter keyFilter = new KeyMappedCorrespondenceFilter(true);
    	log(String.format("Key-Mapped Correspondence filter for %d schema correspondences", schemaCorrespondences.size()));
    	schemaCorrespondences = keyFilter.runBlocking(web.getCandidateKeys(), true, schemaCorrespondences, proc);
    	mf.writeSchemaCorrespondenceGraph(new File(resultsLocationFile.getParentFile(), "schema_graph_filtered.net"), web.getSchema(), schemaCorrespondences, unionGs);
    	mf.writeSchemaCorrespondenceTableGraph(new File(resultsLocationFile.getParentFile(), "schema_table_graph_filtered.net"), web.getSchema(), schemaCorrespondences);
    	mf.printAttributeClusters(schemaCorrespondences);
    	
    	// at this point, there are schema correspondences between two tables only if at least one candidate key can be completely mapped
    	// so tables which are not connected (but might be connected via another table) will definitely produce a denormalised table as result
    	
    	CSVWriter tableStatisticsWriter = new CSVWriter(new FileWriter(new File(resultsLocationFile.getParentFile().getParentFile(), "join_union_reconstruction.csv"), true));
//    	CSVWriter synonymWriter = new CSVWriter(new FileWriter(new File(resultsLocationFile.getParentFile().getParentFile(), "join_union_synonyms.csv"), true));
    	log("Reconstruct tables");
    	Collection<Table> tables = tr.reconstruct(nextTableId, web.getRecords(), web.getSchema(), web.getCandidateKeys(), schemaCorrespondences);
    	for(Table t : tables) {
//    		csvw.write(t, new File(csvResultsLocationFile, t.getPath()));
//    		jsonw.write(t, new File(resultsLocationFile, t.getPath()));
    		log(String.format("Remove sparse columns for %s", t.getPath()));
    		Table dense = tr.removeSparseColumns(t, 0.05);
    		dense.identifyKey(0.001, true);
    		
    		// don't use context columns as keys
    		if(dense.hasKey()) {
    			if(ContextColumns.isContextColumn(dense.getKey())) {
    				dense.setKeyIndex(-1);
    			}
    		}
    		
    		jsonw.write(dense, new File(resultsLocationFile, t.getPath()));
//    		csvw.write(dense, new File(csvResultsLocationFile, t.getPath()));
    		
    		tableStatisticsWriter.writeNext(new String[] {
        			resultsLocationFile.getParentFile().getName(),							// host
    				t.getPath(),															// table name
    				Integer.toString(t.getColumns().size()),								// columns
    				Integer.toString(dense.getColumns().size()),							// dense columns
    				Integer.toString(t.getRows().size())									// rows
//    				stat.generateSchemaString(t),											// schema
//    				stat.generateSchemaString(dense)										// dense schema
        	});
    		
//    		for(TableColumn c : t.getColumns()) {
//    			if(c.getSynonyms().size()>1) {
//    				synonymWriter.writeNext(new String[] {
//    						resultsLocationFile.getParentFile().getName(),
//    	    				t.getPath(),
//    	    				c.getHeader(),
//    	    				StringUtils.join(Q.project(c.getSynonyms(), new Func<String,String>() {
//
//								@Override
//								public String invoke(String in) {
//									return in.replaceAll("\\+", "");
//								}}), "+"),
//    	    				Integer.toString(c.getProvenance().size())
//    				});
//    			}
//    		}
    	}
    	tableStatisticsWriter.close();
//    	synonymWriter.close();
    	
    	end = System.currentTimeMillis();
    	
    	long reconstructionDuration = end-start;
    	System.out.println(String.format("Reconstruction finished after %s", DurationFormatUtils.formatDurationHMS(reconstructionDuration)));
    	
    	TableReport r = new TableReport();
    	r.writeTableReport(tables, new File(resultsLocationFile.getParentFile(), "c3_report.txt"));
    	
    	//statistics: how many determinants for matching, how many include context columns
    	writeStatistics(keyCorrespondences, keyInstanceCorrespondences, tables, matchingDuration, reconstructionDuration, numberOfSynonymClusters, numberOfAttributeClusters);

    	System.out.println("done");
	}

	protected void log(String message) {
		System.out.println(String.format("[%s] %s", DateFormatUtils.format(System.currentTimeMillis(), "HH:mm:ss.mmm"), message));
	}
	
	protected void logDeterminants(ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> determinants) {
		for(Correspondence<MatchableTableKey, MatchableTableColumn> det : determinants.get()) {
			System.out.println(String.format("\t{%s}<->{%s}", 
					StringUtils.join(Q.project(det.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ","), 
					StringUtils.join(Q.project(det.getSecondRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ",")));
		}
	}
	
	protected void logRecordLinks(ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> recordLinks) {
		for(Correspondence<MatchableTableRow, MatchableTableKey> cor : recordLinks.get()) {
			Correspondence<MatchableTableKey, MatchableTableRow> cause = Q.firstOrDefault(cor.getCausalCorrespondences().get());
			
			System.out.println(String.format("\t{%s}<->{%s}", 
					StringUtils.join(Q.project(cause.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ","), 
					StringUtils.join(Q.project(cause.getSecondRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ",")));
			System.out.println(cor.getFirstRecord().format(20));
			System.out.println(cor.getSecondRecord().format(20));
			System.out.println();
		}
	}
	
	protected void writeDisjointHeaders(DisjointHeaders disjointHeaders) throws IOException {
		
		CSVWriter headersWriter = new CSVWriter(new FileWriter(new File(resultsLocationFile.getParentFile().getParentFile(), "join_union_cooccurrence.csv"), true));
		
		Map<String, Map<String, Integer>> disjoint = disjointHeaders.getDisjointCounts();
		Map<String, Map<String, Integer>> disjointOriginal = disjointHeaders.getDisjointCountsOriginal();
		
		for(String header : disjoint.keySet()) {
			
			if(!ContextColumns.isContextColumn(header)) {
			
				Map<String, Integer> counts = disjoint.get(header);
				Map<String, Integer> countsOriginal = disjointOriginal.get(header);
				
				for(String otherHeader : counts.keySet()) {
				
					if(!ContextColumns.isContextColumn(otherHeader)) {
					
						headersWriter.writeNext(new String[] {
							resultsLocationFile.getParentFile().getName(),								// host
							header,																		// header
							otherHeader,																// disjoint header
							Integer.toString(counts.get(otherHeader)),									// co-occurrence count (union tables)
							Integer.toString(countsOriginal.get(otherHeader))							// co-occurrence count (original tables)
						});
					
					}
					
				}
			
			}
			
		}
				
		headersWriter.close();
	}
	
	protected Pair<Integer, Integer> writeCorrespondenceStatistics(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) throws IOException {
		
		HashSet<MatchableTableColumn> nodes = new HashSet<>();
		ConnectedComponentClusterer<MatchableTableColumn> comp = new ConnectedComponentClusterer<>();
		
		ContextColumnsSchemaFilter filter = new ContextColumnsSchemaFilter();
		schemaCorrespondences = filter.run(schemaCorrespondences, proc);
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
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
		
		CSVWriter clusterWriter = new CSVWriter(new FileWriter(new File(resultsLocationFile.getParentFile().getParentFile(), "join_union_clusters.csv"), true));
		CSVWriter synonymWriter = new CSVWriter(new FileWriter(new File(resultsLocationFile.getParentFile().getParentFile(), "join_union_synonyms.csv"), true));
		
		int numAttributeClusters = 0;
		int numSynonymClusters = 0;
		for(Collection<MatchableTableColumn> cluster : clusters.keySet()) {
			
			if(cluster.size()>1) {
				numAttributeClusters++;
			}
			
			Map<String, Integer> numOriginalColumnsPerHeader = new HashMap<>();
			
			int numOriginalColumns = 0;
			for(MatchableTableColumn c : cluster) {
				int originalColumns = web.getTables().get(c.getTableId()).getSchema().get(c.getColumnIndex()).getProvenance().size();
				numOriginalColumns += originalColumns;
				
				MapUtils.add(numOriginalColumnsPerHeader, c.getHeader(), originalColumns);
			}

			String clusterName = MapUtils.max(numOriginalColumnsPerHeader);
			
			clusterWriter.writeNext(new String[] {
					resultsLocationFile.getParentFile().getName(),								// host
					clusterName,																// cluster name
					Integer.toString(cluster.size()),											// cluster size (union columns count)
					Integer.toString(numOriginalColumns)										// cluster size (original columns count)
			});
			
			Distribution<String> headers = Distribution.fromCollection(cluster, new MatchableTableColumn.ColumnHeaderProjection());
			
			if(headers.getNumElements()>1) {
				numSynonymClusters++;
				for(String header : headers.getElements()) {
					if(!"null".equals(header) && !ContextColumns.isContextColumn(header)) {
						synonymWriter.writeNext(new String[] {
								resultsLocationFile.getParentFile().getName(),							// host
								clusterName,															// most frequent header == cluster name
								Integer.toString(cluster.size()),										// cluster size (union columns count)
								Integer.toString(numOriginalColumns),									// cluster size (original columns count)
								header,																	// synonym
								Integer.toString(headers.getFrequency(header)),							// synonym frequency (union columns count)
								Integer.toString(numOriginalColumnsPerHeader.get(header))				// synonym frequency (original columns count)
						});
					}
				}
			}
		}
		
		clusterWriter.close();
		synonymWriter.close();
	
		return new Pair<>(numAttributeClusters, numSynonymClusters);
	}
	
	protected void writeStatistics(ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences, ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences, Collection<Table> reconstructedTables, long matchingDuration, long reconstructionDuration, int numberOfSynonymClusters, int numberOfAttributeClusters) throws IOException {
		
		CSVWriter determinantWriter = new CSVWriter(new FileWriter(new File(resultsLocationFile.getParentFile().getParentFile(), "join_union.csv"), true));
		
		int determinantsIncludingContext = 0;
		
		Set<Collection<MatchableTableColumn>> uniqueDeterminants = new HashSet<>();
		Set<Collection<MatchableTableColumn>> uniqueDeterminantsWithContext = new HashSet<>();
		
		if(keyCorrespondences!=null) {
			for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : keyCorrespondences.get()) {
				uniqueDeterminants.add(cor.getFirstRecord().getColumns());
				uniqueDeterminants.add(cor.getSecondRecord().getColumns());
				
				boolean hasContext = false;
				if(Q.any(cor.getFirstRecord().getColumns(), new ContextColumns.IsMatchableContextColumnPredicate())) {
					hasContext = true;
					uniqueDeterminantsWithContext.add(cor.getFirstRecord().getColumns());
				}
				if(Q.any(cor.getSecondRecord().getColumns(), new ContextColumns.IsMatchableContextColumnPredicate())) {
					hasContext = true;
					uniqueDeterminantsWithContext.add(cor.getSecondRecord().getColumns());
				}
				
				if(hasContext) {
					determinantsIncludingContext++;
				}
			}
		}
		
		int totalDeterminants = 0;
		
		for(Table t : web.getTables().values()) {
			totalDeterminants += t.getSchema().getFunctionalDependencies().size();
		}
		
		int uniqueDeterminantsInt = 0;
		if(uniqueDeterminants!=null)
			uniqueDeterminantsInt = uniqueDeterminants.size();
		
		int uniqueDeterminantsWithContextInt = 0;
		if(uniqueDeterminantsWithContext!=null)
			uniqueDeterminantsWithContextInt = uniqueDeterminantsWithContext.size();
		
		int numReconstructed = 0;
		if(reconstructedTables!=null) 
			numReconstructed = reconstructedTables.size();
		
		int keyCors = 0;
		if(keyCorrespondences!=null)
			keyCors = keyCorrespondences.size();
		
		int duplicates = 0;
		if(keyInstanceCorrespondences!=null) 
			duplicates = keyInstanceCorrespondences.size();
		
		determinantWriter.writeNext(new String[] {
				resultsLocationFile.getParentFile().getName(),							// host
    			Integer.toString(web.getTables().size()),								// union tables
				Integer.toString(numReconstructed),										// reconstructed tables
				Long.toString(matchingDuration),										// matching duration
				Long.toString(reconstructionDuration),									// reconstruction duration
				Integer.toString(totalDeterminants),									// total number of FDs/determinants
				Integer.toString(uniqueDeterminantsInt),								// number of unique determinants matched
				Integer.toString(uniqueDeterminantsWithContextInt),						// number of unique determinants matched that include context columns
				Integer.toString(keyCors),												// total number of determinant matches
				Integer.toString(determinantsIncludingContext),							// total number of determinant matches including context columns
				Integer.toString(duplicates),											// number of duplicates
				Integer.toString(numberOfAttributeClusters),							// number of attribute clusters
				Integer.toString(numberOfSynonymClusters)								// number of synonym clusters
		});
		
		determinantWriter.close();
	}
}
