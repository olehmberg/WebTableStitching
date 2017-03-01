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
package de.uni_mannheim.informatik.dws.tnt.match.tasks.matching;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DurationFormatUtils;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.graph.Graph;
import de.uni_mannheim.informatik.dws.t2k.utils.data.graph.Partitioning;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.rules.SchemaSynonymBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaTransitivityAggregator;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public abstract class TableMatchingTask extends TnTTask {

	public String getTaskName() {
		return this.getClass().getSimpleName();
	}
	
	private long runtime = 0;
	/**
	 * @return the runtime
	 */
	public long getRuntime() {
		return runtime;
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
	
	protected WebTables web;
	protected File resultsLocationFile;
	protected File evaluationLocation;
	protected N2NGoldStandard gs;
	protected N2NGoldStandard unionGs;
	protected Map<String, Set<String>> disjointHeaders;
	
	/**
	 * @param disjointHeaders the disjointHeaders to set
	 */
	public void setDisjointHeaders(Map<String, Set<String>> disjointHeaders) {
		this.disjointHeaders = disjointHeaders;
	}
	/**
	 * @return the disjointHeaders
	 */
	public Map<String, Set<String>> getDisjointHeaders() {
		return disjointHeaders;
	}
	
	/**
	 * @param web the web to set
	 */
	public void setWebTables(WebTables web) {
		this.web = web;
	}
	
	protected ClusteringPerformance schemaPerformance;
	/**
	 * @return the performance
	 */
	public ClusteringPerformance getSchemaPerformance() {
		return schemaPerformance;
	}
	
	protected ClusteringPerformance unionSchemaPerformance;
	/**
	 * @return the unionSchemaPerformance
	 */
	public ClusteringPerformance getUnionSchemaPerformance() {
		return unionSchemaPerformance;
	}
	
	protected ClusteringPerformance unionSchemaPerformanceInverse;
	
	/**
	 * @return the unionSchemaPerformanceInverse
	 */
	public ClusteringPerformance getUnionSchemaPerformanceInverse() {
		return unionSchemaPerformanceInverse;
	}
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = null;
	/**
	 * @return the schemaCorrespondences
	 */
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> getSchemaCorrespondences() {
		return schemaCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = null;
	/**
	 * @return the keyCorrespondences
	 */
	public ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> getKeyCorrespondences() {
		return keyCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = null;
	/**
	 * @return the keyInstanceCorrespondences
	 */
	public ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> getKeyInstanceCorrespondences() {
		return keyInstanceCorrespondences;
	}
	
	public void initialise() throws IOException {
		if(web==null)  {
			// load web tables
			web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		}
		
		// prepare output directories
    	resultsLocationFile = new File(new File(resultLocation), "join_union");
    	setOutputDirectory(resultsLocationFile);
    	resultsLocationFile.mkdirs();

    	evaluationLocation = new File(new File(resultLocation), "evaluation");
    	evaluationLocation.mkdirs();
    
    	gs = new N2NGoldStandard();
    	File gsFile = new File(evaluationLocation, "goldstandard.tsv");
    	if(gsFile.exists()) {
	    	gs.loadFromTSV(gsFile);
    	}
    	
    	unionGs = new N2NGoldStandard();
    	gsFile = new File(evaluationLocation, "union_goldstandard.tsv");
    	if(gsFile.exists()) {
    		unionGs.loadFromTSV(gsFile);
    	}
	}
	
	public void evaluateBaseline() {
		Set<String> baseline = new HashSet<>();
		for(Set<String> clu : unionGs.getCorrespondenceClusters().keySet()) {
			baseline.addAll(clu);
		}
		
		// create baseline for all attributes in one cluster
		Map<Set<String>, String> map = new HashMap<>();
		map.put(baseline, "all");
		ClusteringPerformance baselineResult = unionGs.evaluateCorrespondenceClusters(map, true);
		unionSchemaPerformance = baselineResult;
		System.out.println("Baseline on Union Gold Standard:");
		System.out.println(baselineResult.format(false));
		System.out.println("Baseline on Union Gold Standard (Inverse):");
		baselineResult = unionGs.evaluateCorrespondenceClustersInverse(map.keySet(), false);
//		System.out.println(baselineResult.format(false));
		unionSchemaPerformanceInverse = baselineResult;
		
		// create baseline for no result
		baseline.clear();
		baselineResult = unionGs.evaluateCorrespondenceClusters(map, true);
		System.out.println("Empty Baseline on Union Gold Standard:");
		System.out.println(baselineResult.format(false));
		System.out.println("Empty Baseline on Union Gold Standard (Inverse):");
		baselineResult = unionGs.evaluateCorrespondenceClustersInverse(map.keySet(), false);
//		System.out.println(baselineResult.format(false));
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.TnTTask#match()
	 */
	@Override
	public void match() throws Exception {
		long start = System.currentTimeMillis();
		
		runMatching();
		
    	long end = System.currentTimeMillis();
    	
    	runtime = end-start;
    	
    	System.out.println(String.format("[%s] Matching finished after %s", this.getClass().getName(), DurationFormatUtils.formatDurationHMS(runtime)));
	}
	
	public abstract void runMatching() throws Exception;

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
		
		for(MatchableTableColumn node : web.getSchema().get()) {
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
			
			if(originalColumns.size()>0) {
				n2n.getCorrespondenceClusters().put(originalColumns, Q.firstOrDefault(originalColumns));
			}
		}
		
		return n2n;
	}
	
	public N2NGoldStandard createMappingForUnionColumns(Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences) {
		HashSet<MatchableTableColumn> nodes = new HashSet<>();
		ConnectedComponentClusterer<MatchableTableColumn> comp = new ConnectedComponentClusterer<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : correspondences) {
			if(cor.getSimilarityScore()>0.0) {
				nodes.add(cor.getFirstRecord());
				nodes.add(cor.getSecondRecord());
				
				comp.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
			}
		}
		
		for(MatchableTableColumn node : web.getSchema().get()) {
			comp.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(node, node, 1.0));
		}
		
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clusters = comp.createResult();
		N2NGoldStandard n2n = new N2NGoldStandard();
		
		for(Collection<MatchableTableColumn> cluster : clusters.keySet()) {
			HashSet<String> unionColumns = new HashSet<>();
			
			for(MatchableTableColumn c : cluster) {
				if(!SpecialColumns.isSpecialColumn(c) && !ContextColumns.isContextColumn(c)) {
					TableColumn col = web.getTables().get(c.getTableId()).getSchema().get(c.getColumnIndex());
//					unionColumns.add(col.getProvenanceString());
					unionColumns.add(col.getIdentifier());
				}
			}
			
			if(unionColumns.size()>0) {
				n2n.getCorrespondenceClusters().put(unionColumns, Q.firstOrDefault(unionColumns));
			}
		}
		
		return n2n;
	}
	
	public Collection<Pair<String, String>> createCorrespondencesForUnionColumns(Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences) {
		Collection<Pair<String, String>> result = new ArrayList<>(correspondences.size());
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : correspondences) {
			if(!ContextColumns.isContextColumn(cor.getFirstRecord()) && !ContextColumns.isContextColumn(cor.getSecondRecord())
					&& !SpecialColumns.isSpecialColumn(cor.getFirstRecord()) && !SpecialColumns.isSpecialColumn(cor.getSecondRecord())) {
				TableColumn col1 = web.getTables().get(cor.getFirstRecord().getTableId()).getSchema().get(cor.getFirstRecord().getColumnIndex());
				TableColumn col2 = web.getTables().get(cor.getSecondRecord().getTableId()).getSchema().get(cor.getSecondRecord().getColumnIndex());
				result.add(new Pair<String, String>(col1.getProvenanceString(), col2.getProvenanceString()));
			}
		}
		
		return result;
	}
	
	protected void evaluateSchemaCorrespondences(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		
		schemaCorrespondences.deduplicate();
		
		printSchemaSynonyms(schemaCorrespondences);
		
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> corsWithoutContext = new ResultSet<>();
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
    		if(!SpecialColumns.isSpecialColumn(cor.getFirstRecord() )&& !SpecialColumns.isSpecialColumn(cor.getSecondRecord())
    				&& !ContextColumns.isContextColumn(cor.getFirstRecord()) && !ContextColumns.isContextColumn(cor.getSecondRecord())) {
    			corsWithoutContext.add(cor);
    		}
    	}
		
//    	N2NGoldStandard schemaMapping = createMappingForOriginalColumns(schemaCorrespondences.get());
//    	schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
//    	schemaMapping = createMappingForUnionColumns(schemaCorrespondences.get());
//    	System.out.println(String.format("%d schema correspondences", schemaCorrespondences.size()));
//    	System.out.println(String.format("%d attribute clusters", schemaMapping.getCorrespondenceClusters().size()));
//    	printAttributeClusters(schemaMapping.getCorrespondenceClusters().keySet());
//    	unionSchemaPerformance = unionGs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	N2NGoldStandard schemaMapping = createMappingForOriginalColumns(corsWithoutContext.get());
    	schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
    	
    	schemaMapping = createMappingForUnionColumns(corsWithoutContext.get());
    	System.out.println(String.format("%d schema correspondences", corsWithoutContext.size()));
    	System.out.println(String.format("%d attribute clusters", schemaMapping.getCorrespondenceClusters().size()));
    	printAttributeClusters(schemaMapping.getCorrespondenceClusters().keySet());
    	unionSchemaPerformance = unionGs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	
    	
    	unionSchemaPerformance.setCorrespondencePerformance(unionGs.evaluateCorrespondencePerformance(corsWithoutContext.get(), false));
    	
    	System.out.println("All correspondences");
    	logMatrixPerHeader(corsWithoutContext.get());
    	System.out.println("Correct");
//    	System.out.println(SimilarityMatrix.fromCorrespondences(unionGs.getCorrectCorrespondences(corsWithoutContext.get()), new SparseSimilarityMatrixFactory()).getOutput());
    	logMatrixPerHeader(unionGs.getCorrectCorrespondences(corsWithoutContext.get()));
    	System.out.println("Incorrect");
//    	System.out.println(SimilarityMatrix.fromCorrespondences(unionGs.getIncorrectCorrespondences(corsWithoutContext.get()), new SparseSimilarityMatrixFactory()).getOutput());
    	logMatrixPerHeader(unionGs.getIncorrectCorrespondences(corsWithoutContext.get()));
    	
    	SchemaTransitivityAggregator transitivity = new SchemaTransitivityAggregator();
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> transitiveCors = transitivity.aggregate(schemaCorrespondences, proc);
    	transitiveCors = proc.append(transitiveCors, schemaCorrespondences);
    	transitiveCors.deduplicate();
//    	unionSchemaPerformance.setTransitiveCorrespondencePerformance(unionGs.evaluateCorrespondencePerformance(createCorrespondencesForUnionColumns(transitiveCors.get()), true));
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> transitiveCorsWithoutContext = new ResultSet<>();
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : transitiveCors.get()) {
    		if(!SpecialColumns.isSpecialColumn(cor.getFirstRecord() )&& !SpecialColumns.isSpecialColumn(cor.getSecondRecord())
    				&& !ContextColumns.isContextColumn(cor.getFirstRecord()) && !ContextColumns.isContextColumn(cor.getSecondRecord())) {
    			transitiveCorsWithoutContext.add(cor);
    		}
    	}
    	unionSchemaPerformance.setTransitiveCorrespondencePerformance(unionGs.evaluateCorrespondencePerformance(transitiveCorsWithoutContext.get(), false));
//    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : corsWithoutContext.get()) {
//    		if(!unionGs.isCorrectCorrespondence(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier())) {
//    			System.out.println(String.format("[incorrect] %s<->%s", cor.getFirstRecord(), cor.getSecondRecord()));
//    			if(cor.getCausalCorrespondences()!=null) {
//	    			for(Correspondence<MatchableTableRow, MatchableTableColumn> cause : cor.getCausalCorrespondences().get()) {
//	    				System.out.println(String.format("\t%s\n\t%s", cause.getFirstRecord().format(20), cause.getSecondRecord().format(20)));
//	    				if(cause.getCausalCorrespondences()!=null) {
//	    					for(Correspondence<MatchableTableColumn, MatchableTableRow> causeCause : cause.getCausalCorrespondences().get()) {
//	    						System.out.println(String.format("\t\t%s<->%s", causeCause.getFirstRecord(), causeCause.getSecondRecord()));
//	    					}
//	    				}
//	    			}
//    			}
//    		}
//    	}
    	
    	unionSchemaPerformanceInverse = unionGs.evaluateCorrespondenceClustersInverse(schemaMapping.getCorrespondenceClusters().keySet(), false);
//    	System.out.println(unionSchemaPerformanceInverse.format(true));
    	
    	try {
			writeSchemaCorrespondenceGraph(new File(resultsLocationFile.getParentFile(), getTaskName() + "_schema.net"), web.getSchema(), transitiveCorsWithoutContext);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected <T> void logMatrixPerHeader(Collection<Correspondence<MatchableTableColumn, T>> correspondences) {
		SimilarityMatrix<String> perHeader = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
		
		for(Correspondence<MatchableTableColumn, T> cor : correspondences) {
			Double current = perHeader.get(cor.getFirstRecord().getHeader(), cor.getSecondRecord().getHeader());
			if(current==null) {
				current = 0.0;
			}
			current = Math.max(current, cor.getSimilarityScore());
			perHeader.set(cor.getFirstRecord().getHeader(), cor.getSecondRecord().getHeader(), current);
		}
		
		System.out.println(perHeader.getOutput());
	}
	
	protected void logMatrixPerHeaderFromKeys(Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> correspondences) {
		
		Collection<Correspondence<MatchableTableColumn, MatchableTableKey>> attributeCors = new LinkedList<>();
		
		for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : correspondences) {
			attributeCors.addAll(cor.getCausalCorrespondences().get());
		}
		
		logMatrixPerHeader(attributeCors);
	}
	
	protected void logKeyMatrixPerHeader(Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> correspondences) {
		
		SimilarityMatrix<String> m = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
		
		for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : correspondences) {
			Double current = m.get(cor.getFirstRecord().getColumns().toString(), cor.getSecondRecord().getColumns().toString());
			if(current==null) {
				current = 0.0;
			}
			m.set(cor.getFirstRecord().getColumns().toString(), cor.getSecondRecord().getColumns().toString(), current+1.0);
		}
		
		System.out.println(m.getOutput(100));
	}
	
	protected void logKeyMatrixPerHeaderFromRecordLinks(Collection<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences) {
		
		SimilarityMatrix<String> m = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
		
		for(Correspondence<MatchableTableRow, MatchableTableKey> cor : correspondences) {
			
			for(Correspondence<MatchableTableKey, MatchableTableRow> keyCor : cor.getCausalCorrespondences().get()) {
				Double current = m.get(keyCor.getFirstRecord().getColumns().toString(), keyCor.getSecondRecord().getColumns().toString());
				if(current==null) {
					current = 0.0;
				}
				m.set(keyCor.getFirstRecord().getColumns().toString(), keyCor.getSecondRecord().getColumns().toString(), current+1.0);
			}
		}
		
		System.out.println(m.getOutput(100));
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
	
	private void printAttributeClusters(Set<Set<String>> clusters) {
		for(Set<String> clu :  clusters) {
			Set<String> names = new HashSet<>();
			for(String s : clu) {
				MatchableTableColumn c = web.getSchema().getRecord(s);
//				names.add(s.split(";")[2]);
				names.add(c.getHeader());
			}
			if(names.size()>1) {
				System.out.println(String.format("\t%s", StringUtils.join(names, ",")));
			}
		}
	}
	
	protected void printKeyCorrespondences(Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences) {
		
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
	
	public void writeSchemaCorrespondenceGraph(File f, DataSet<MatchableTableColumn, MatchableTableColumn> allAttributes, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) throws IOException {
		Graph<MatchableTableColumn, Object> g = new Graph<>();
		
		// map the column provenance string (in the gold standard) to the column object
		Map<String, MatchableTableColumn> columns = new HashMap<>();
		
		for(MatchableTableColumn c : allAttributes.get()) {
			if(!SpecialColumns.isSpecialColumn(c)) {
				g.addNode(c);
				columns.put(web.getTables().get(c.getTableId()).getSchema().get(c.getColumnIndex()).getProvenanceString(), c);
			}
		}
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			g.addEdge(cor.getFirstRecord(), cor.getSecondRecord(), cor, cor.getSimilarityScore());
		}
	
		g.writePajekFormat(f);
		
		List<Set<String>> gsPartitions = new ArrayList<>(unionGs.getCorrespondenceClusters().keySet());
		Partitioning<MatchableTableColumn> partitioning = new Partitioning<>(g);
		
		for(int i = 0; i < gsPartitions.size(); i++) {
			
			Set<String> partition = gsPartitions.get(i);
			
			for(String column : partition) {
				MatchableTableColumn c = columns.get(column);
				
				partitioning.setPartition(c, i+1);
			}
			
		}
			
		partitioning.writePajekFormat(new File(f.getAbsolutePath() + ".goldstandard.clu"));	
	}
	
	public void writeRecordLinkageGraph(File f, ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences, int tableId1, int tableId2) throws IOException {
		Graph<String, Object> g = new Graph<>();
		Partitioning<String> tablePartitioning = new Partitioning<>(g);
		Partitioning<String> goldPartitioning = new Partitioning<>(g);
		
		Set<String> tablesWithTrackNr = Q.toSet("42729.json,42732.json,42734.json,42738.json,42742.json,42743.json,42745.json,42747.json,42748.json,42749.json,42751.json".split(","));
		
		
		for(Correspondence<MatchableTableRow, MatchableTableKey> cor : correspondences.get()) {
			
			if(cor.getFirstRecord().getTableId()==tableId1 || cor.getFirstRecord().getTableId()==tableId2
					|| cor.getSecondRecord().getTableId()==tableId1 || cor.getSecondRecord().getTableId()==tableId2
					|| tableId1==-1 || tableId2==-1) {
				
				
					String n1 = cor.getFirstRecord().toString();
					String n2 = cor.getSecondRecord().toString();
				
					StringBuilder edge = new StringBuilder();
					for(Correspondence<MatchableTableKey, MatchableTableRow> cause : cor.getCausalCorrespondences().get()) {
//						String n1 = String.format("%s %s", cor.getFirstRecord(), cause.getFirstRecord().getColumns());
//						String n2 = String.format("%s %s", cor.getSecondRecord(), cause.getSecondRecord().getColumns());
						if(edge.length()!=0) {
							edge.append(" || ");
						}
						edge.append(String.format("%s", cause.getFirstRecord().getColumns()));
					}
					
					g.addEdge(n1, n2, edge, cor.getCausalCorrespondences().size());
					tablePartitioning.setPartition(n1, cor.getFirstRecord().getTableId());
					tablePartitioning.setPartition(n2, cor.getSecondRecord().getTableId());
					
					Table t1 = web.getTables().get(cor.getFirstRecord().getTableId());
					Table t2 = web.getTables().get(cor.getSecondRecord().getTableId());
					
					if(tablesWithTrackNr.contains(t1.getPath())) {
						goldPartitioning.setPartition(n1, 1);
					} else {
						goldPartitioning.setPartition(n1, 2);
					}
					if(tablesWithTrackNr.contains(t2.getPath())) {
						goldPartitioning.setPartition(n2, 1);
					} else {
						goldPartitioning.setPartition(n2, 2);
					}
				
			}
			
		}
		
		g.writePajekFormat(f);
		tablePartitioning.writePajekFormat(new File(f.getAbsolutePath() + ".tables.clu"));
		goldPartitioning.writePajekFormat(new File(f.getAbsolutePath() + ".gold.clu"));
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
}
