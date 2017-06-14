package de.uni_mannheim.informatik.dws.tnt.match.evaluation;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.SchemaSynonymsClusterer;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.dws.winter.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

public class TableToTableEvaluator {
	
	public ClusteringPerformance evaluateSchemaCorrespondences(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, WebTables web, File evaluationLocation) throws IOException {
		
		TableToTableEvaluator eval = new TableToTableEvaluator();
		N2NGoldStandard gs;
		N2NGoldStandard unionGs;
		
    	gs = new N2NGoldStandard();
    	File gsFile = new File(evaluationLocation, "goldstandard.tsv");
    	if(gsFile.exists()) {
	    	gs.loadFromTSV(gsFile);
    	} else {
    		System.err.println(String.format("File %s does not exist!", gsFile.getAbsolutePath()));
    	}
    	
    	unionGs = new N2NGoldStandard();
    	gsFile = new File(evaluationLocation, "union_goldstandard.tsv");
    	if(gsFile.exists()) {
    		unionGs.loadFromTSV(gsFile);
    	} else {
    		System.err.println(String.format("File %s does not exist!", gsFile.getAbsolutePath()));
    	}
		
		printSchemaSynonyms(schemaCorrespondences, web);
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> corsWithoutContext = new ProcessableCollection<>();
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
    		if(!SpecialColumns.isSpecialColumn(cor.getFirstRecord() )&& !SpecialColumns.isSpecialColumn(cor.getSecondRecord())
    				&& !ContextColumns.isContextColumn(cor.getFirstRecord()) && !ContextColumns.isContextColumn(cor.getSecondRecord())) {
    			corsWithoutContext.add(cor);
    		}
    	}

    	N2NGoldStandard schemaMapping = null;

    	schemaMapping = eval.createMappingForUnionColumns(corsWithoutContext.get(), web);
    	System.out.println(String.format("%d schema correspondences", corsWithoutContext.size()));
    	System.out.println(String.format("%d attribute clusters", schemaMapping.getCorrespondenceClusters().size()));
    	printAttributeClusters(schemaMapping.getCorrespondenceClusters().keySet(), web);
    	ClusteringPerformance unionSchemaPerformance = unionGs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), true);
    	
    	unionSchemaPerformance.setCorrespondencePerformance(unionGs.evaluateCorrespondencePerformance(corsWithoutContext.get(), false));
    	
		return unionSchemaPerformance;
	}
	
	protected void printSchemaSynonyms(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, WebTables web) {	
    	// add schema correspondences via attribute names
    	SchemaSynonymsClusterer generateSynonyms = new SchemaSynonymsClusterer();
    	Processable<Set<String>> synonyms = generateSynonyms.runClustering(web.getSchema(), true, schemaCorrespondences);
    	System.out.println("Synonyms");
    	for(Set<String> clu : synonyms.get()) {
    		System.out.println("\t" + StringUtils.join(clu, ","));
    	}
	}
	
	private void printAttributeClusters(Set<Set<String>> clusters, WebTables web) {
		for(Set<String> clu :  clusters) {
			Set<String> names = new HashSet<>();
			for(String s : clu) {
				MatchableTableColumn c = web.getSchema().getRecord(s);
				names.add(c.getHeader());
			}
			if(names.size()>1) {
				System.out.println(String.format("\t%s", StringUtils.join(names, ",")));
			}
		}
	}
	
	protected void logHeaderMatrix(Collection<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		SimilarityMatrix<String> perHeader = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : correspondences) {
			Double current = perHeader.get(cor.getFirstRecord().getHeader(), cor.getSecondRecord().getHeader());
			if(current==null) {
				current = 0.0;
			}
			current = Math.max(current, cor.getSimilarityScore());
			perHeader.set(cor.getFirstRecord().getHeader(), cor.getSecondRecord().getHeader(), current);
		}
		
		System.out.println(perHeader.getOutput());
	}

	public N2NGoldStandard createMappingForOriginalColumns(Collection<Correspondence<MatchableTableColumn, Matchable>> correspondences, WebTables web) {
		HashSet<MatchableTableColumn> nodes = new HashSet<>();
		ConnectedComponentClusterer<MatchableTableColumn> comp = new ConnectedComponentClusterer<>();
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : correspondences) {
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

	public N2NGoldStandard createMappingForUnionColumns(Collection<Correspondence<MatchableTableColumn, Matchable>> correspondences, WebTables web) {
		HashSet<MatchableTableColumn> nodes = new HashSet<>();
		ConnectedComponentClusterer<MatchableTableColumn> comp = new ConnectedComponentClusterer<>();
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : correspondences) {
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
					unionColumns.add(col.getIdentifier());
				}
			}
			
			if(unionColumns.size()>0) {
				n2n.getCorrespondenceClusters().put(unionColumns, Q.firstOrDefault(unionColumns));
			}
		}
		
		return n2n;
	}
}
