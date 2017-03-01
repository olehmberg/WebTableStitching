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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.t2k.utils.data.Pair;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.matrices.ArrayBasedSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.wdi.model.Performance;
import de.uni_mannheim.informatik.wdi.parallel.ParallelDataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableReMapping extends TnTTask {


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
		TableReMapping trm = new TableReMapping();

		if (trm.parseCommandLine(TableReMapping.class, args)) {

			hello();

			trm.initialise();
			trm.setDataProcessingEngine(new ParallelDataProcessingEngine());
			trm.setMatchingEngine(new MatchingEngine<MatchableTableRow, MatchableTableColumn>());

			trm.match();

		}
	}

	private WebTables web;
	private File resultsLocationFile;
	private File evaluationLocationFile;
	
	public void initialise() throws IOException {
		printHeadline("Table Re-Mapping");
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		
		// prepare output directories
    	resultsLocationFile = new File(new File(resultLocation), "remapping");
    	setOutputDirectory(resultsLocationFile);
    	resultsLocationFile.mkdirs();
    	
    	
    	
    	evaluationLocationFile = new File(new File(resultLocation), "evaluation");
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.TnTTask#match()
	 */
	@Override
	public void match() throws Exception {
	
		// load the t2k mapping correspondences
		N2NGoldStandard t2k = new N2NGoldStandard();
		t2k.loadFromTSV(new File(evaluationLocationFile, "t2k_correspondences.tsv"));
		
		Map<Set<String>, String> columnClusters = new HashMap<Set<String>, String>();
		// find the largest overlap between each column's provenance and the t2k mapping cluster
		for(Table t : web.getTables().values()) {
			for(TableColumn c : t.getColumns()) {
				if(!SpecialColumns.isSpecialColumn(c)) {
					
					Set<String> prov = new HashSet<>(c.getProvenance());
					
					columnClusters.put(prov, c.getIdentifier());
				}
			}
		}
		
		SimilarityMatrix<String> combinations = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(columnClusters.size(), t2k.getCorrespondenceClusters().size());
		
		for(Set<String> gsCluster : t2k.getCorrespondenceClusters().keySet()) {
			
			int overlapSize = 0;
			Set<String> bestMatch = null;
			
			// find the cluster with the largest overlap
			for(Set<String> cluster : columnClusters.keySet()) {
				
				int intersectionSize = Q.intersection(gsCluster, cluster).size();
				
				bestMatch = cluster;
				overlapSize = intersectionSize;
				
				int matchSize = bestMatch==null ? 0 : bestMatch.size();
				
				// matchSize is the size of the merged column's provenance
				// gsCluster is the size of the t2k correspondences cluster
				// we can use recall of the t2k cluster wrt. the column's provenance as quality measure
				Performance perf = new Performance(overlapSize, gsCluster.size(), matchSize);
				
				String dbpProp = t2k.getCorrespondenceClusters().get(gsCluster);
				String column = columnClusters.get(bestMatch);
				
				//for debug
//				dbpProp = dbpProp.replace("http://dbpedia.org/ontology/", "");
				
				combinations.set(column, dbpProp, perf.getRecall());
				
				
//				Table tbl = null;
				TableColumn c = null;
				for(Table t : web.getTables().values()) {
					c = t.getSchema().getRecord(column);
					if(c!=null) {
//						tbl = t;
						break;
					}
				}
//				System.out.println(String.format("{%s}[%d]%s <-> %s: Column Provenance: %d columns / T2K Cluster: %d columns / Overlap: %d columns / P: %.4f / R: %.4f", c.getTable().getPath(), c.getColumnIndex(), c.getHeader(), dbpProp, matchSize, gsCluster.size(), overlapSize, perf.getPrecision(), perf.getRecall()));
				
		
			}
			

		}

		System.out.println(combinations.getOutput());

		for(String column : combinations.getFirstDimension()) {
			System.out.print(String.format("%s: ", column));
			for(String dbpProp : combinations.getMatchesAboveThreshold(column, 0.0)) {
				System.out.print(String.format("%s (%.6f), ", dbpProp.replace("http://dbpedia.org/ontology/", ""), combinations.get(column, dbpProp)));
			}
			System.out.println();
		}
		
		BestChoiceMatching bestChoice = new BestChoiceMatching();
		combinations = bestChoice.match(combinations);
		
		for(String column : combinations.getFirstDimension()) {
			for(String dbpProp : combinations.getMatchesAboveThreshold(column, 0.0)) {
				Table tbl = null;
				TableColumn c = null;
				for(Table t : web.getTables().values()) {
					c = t.getSchema().getRecord(column);
					if(c!=null) {
						tbl = t;
						break;
					}
				}
				
				if(c!=null) {
					// map the column to that property (maybe use some min threshold)
					tbl.getMapping().setMappedProperty(c.getColumnIndex(), new Pair<String, Double>(dbpProp, combinations.get(column, dbpProp)));
					System.out.println(String.format("Mapped {%s}[%d]%s to %s", c.getTable().getPath(), c.getColumnIndex(), c.getHeader(), dbpProp));
				}
			}
		}
		
		// now evaluate the new mapping against the goldstandard
    	N2NGoldStandard remapped = new N2NGoldStandard();
    	for(Table t : web.getTables().values()) {
    		for(TableColumn c : t.getColumns()) {
    			Pair<String, Double> mapping = t.getMapping().getMappedProperty(c.getColumnIndex());
    			if(mapping!=null) {
    				remapped.getCorrespondenceClusters().put(new HashSet<>(c.getProvenance()), String.format("{%s}[%d]%s", t.getPath(), c.getColumnIndex(), c.getHeader()));
    			}
    		}
    	}
    	
    	N2NGoldStandard gs = new N2NGoldStandard();
    	gs.loadFromTSV(new File(evaluationLocationFile, "goldstandard_dbp.tsv"));
    	
    	//evaluate T2K correspondences
    	printHeadline("Evaluating original T2K correspondences");
    	ClusteringPerformance perf = gs.evaluateCorrespondenceClusters(t2k.getCorrespondenceClusters(), false);
    	System.out.println(gs.formatEvaluationResult(perf.getPerformanceByCluster(), false));
    	
    	//evaluate remapped correspondences
		printHeadline("Evaluating remapped T2K correspondences");
		perf = gs.evaluateCorrespondenceClusters(remapped.getCorrespondenceClusters(), false);
    	System.out.println(gs.formatEvaluationResult(perf.getPerformanceByCluster(), false));
    	
    	// write the results
		WebTables.writeTables(web.getTables().values(), resultsLocationFile, null);
	}

}
