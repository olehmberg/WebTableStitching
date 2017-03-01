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
package de.uni_mannheim.informatik.dws.tnt.match.evaluation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.MappingFormatter;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaTransitivityAggregator;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class N2NEvaluator {

	protected N2NGoldStandard createMappingForOriginalColumns(Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences, WebTables web) {
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
	
	protected N2NGoldStandard createMappingForUnionColumns(Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences, WebTables web) {
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
	
	protected Collection<Pair<String, String>> createCorrespondencesForUnionColumns(Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences, WebTables web) {
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
	
	public ClusteringPerformance evaluateSchemaCorrespondences(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, WebTables web, DataProcessingEngine proc, N2NGoldStandard gs) {
		
		MappingFormatter mf = new MappingFormatter();
		
		mf.printSchemaSynonyms(web.getSchema(), schemaCorrespondences, proc);
		
//    	N2NGoldStandard schemaMapping = createMappingForOriginalColumns(schemaCorrespondences.get(), web);
//    	ClusteringPerformance schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
    	N2NGoldStandard schemaMapping = createMappingForUnionColumns(schemaCorrespondences.get(), web);
//    	System.out.println(String.format("%d schema correspondences", schemaCorrespondences.size()));
//    	System.out.println(String.format("%d attribute clusters", schemaMapping.getCorrespondenceClusters().size()));
//    	printAttributeClusters(schemaMapping.getCorrespondenceClusters().keySet());
    	
    	
    	ClusteringPerformance schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
//    	unionSchemaPerformance.setCorrespondencePerformance(unionGs.evaluateCorrespondencePerformance(createCorrespondencesForUnionColumns(schemaCorrespondences.get()), false));
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> corsWithoutContext = new ResultSet<>();
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
    		if(!SpecialColumns.isSpecialColumn(cor.getFirstRecord() )&& !SpecialColumns.isSpecialColumn(cor.getSecondRecord())
    				&& !ContextColumns.isContextColumn(cor.getFirstRecord()) && !ContextColumns.isContextColumn(cor.getSecondRecord())) {
    			corsWithoutContext.add(cor);
    		}
    	}
    	schemaPerformance.setCorrespondencePerformance(gs.evaluateCorrespondencePerformance(corsWithoutContext.get(), false));
    	
    	SchemaTransitivityAggregator transitivity = new SchemaTransitivityAggregator();
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> transitiveCors = transitivity.aggregate(schemaCorrespondences, proc);
    	transitiveCors = proc.append(transitiveCors, schemaCorrespondences);
    	transitiveCors.deduplicate();
//    	unionSchemaPerformance.setTransitiveCorrespondencePerformance(unionGs.evaluateCorrespondencePerformance(createCorrespondencesForUnionColumns(transitiveCors.get()), true));
    	corsWithoutContext = new ResultSet<>();
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : transitiveCors.get()) {
    		if(!SpecialColumns.isSpecialColumn(cor.getFirstRecord() )&& !SpecialColumns.isSpecialColumn(cor.getSecondRecord())
    				&& !ContextColumns.isContextColumn(cor.getFirstRecord()) && !ContextColumns.isContextColumn(cor.getSecondRecord())) {
    			corsWithoutContext.add(cor);
    		}
    	}
    	schemaPerformance.setTransitiveCorrespondencePerformance(gs.evaluateCorrespondencePerformance(corsWithoutContext.get(), false));
    	
    	return schemaPerformance;
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
    	
//    	unionSchemaPerformanceInverse = unionGs.evaluateCorrespondenceClustersInverse(schemaMapping.getCorrespondenceClusters().keySet(), false);
////    	System.out.println(unionSchemaPerformanceInverse.format(true));
//    	
//    	try {
//			writeSchemaCorrespondenceGraph(new File(resultsLocationFile, getTaskName() + "_schema.net"), web.getSchema(), schemaCorrespondences);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
	
}
