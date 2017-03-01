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
package de.uni_mannheim.informatik.dws.tnt.match.tasks.experiments;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.DateTime;

import com.beust.jcommander.Parameter;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.AllBaselinesTableMatching;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.OneClusterMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.DuplicateBasedTableMatching;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.EntityLabelBasedMatching;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.LabelBasedTableMatching;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.NoClusterMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.ValueBasedTableMatching;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.parallel.ParallelMatchingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchingExperiments extends TnTTask {

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
	protected Map<String, Set<String>> disjointHeaders;

	public static void main(String[] args) throws Exception {
		MatchingExperiments tju = new MatchingExperiments();

		if (tju.parseCommandLine(MatchingExperiments.class, args)) {

			hello();

			tju.initialise();
			tju.setMatchingEngine(new ParallelMatchingEngine<MatchableTableRow, MatchableTableColumn>());
//			tju.setMatchingEngine(new MatchingEngine<MatchableTableRow, MatchableTableColumn>());
			tju.setDataProcessingEngine(tju.matchingEngine.getProcessingEngine());
			
			tju.match();

		}
	}
	

	public void initialise() throws Exception {
		printHeadline("Matching Baselines");
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		web.removeHorizontallyStackedTables();
		
		// prepare output directories
    	resultsLocationFile = new File(new File(resultLocation), "join_union");
    	setOutputDirectory(resultsLocationFile);
    	resultsLocationFile.mkdirs();
    	
//    	disjointHeaders = new HashMap<>();
//    	for(Table t : web.getTables().values()) {
//    		for(int i = 0; i < t.getColumns().size(); i++) {
//    			TableColumn c1 = t.getSchema().get(i);
//    			
//    			if(!c1.getHeader().equals("null") && !c1.getHeader().isEmpty()) {
//    				Set<String> disjoint = MapUtils.get(disjointHeaders, c1.getHeader(), new HashSet<String>());
//    				
//	    			for(int j = 0; j < t.getColumns().size(); j++) {
//	    				TableColumn c2 = t.getSchema().get(j);
//	    				if(i!=j && !c2.getHeader().equals("null") && !c2.getHeader().isEmpty()) {
//	    					//TODO don't add headers that appear multiple times in this table!
//	    					disjoint.add(c2.getHeader());
//	    				}
//	    			}
//    			}
//    		}
//    	}
    	disjointHeaders = DisjointHeaders.fromTables(web.getTables().values()).getAllDisjointHeaders();
    	
    	System.out.println("Disjoint Headers:");
    	for(String header : disjointHeaders.keySet()) {
    		Set<String> disjoint = disjointHeaders.get(header);
    		System.out.println(String.format("\t%s\t%s", header, StringUtils.join(disjoint, ",")));
    	}
	}
	
	public void match() throws Exception {
	
    	// print table ids
    	for(Integer id : Q.sort(web.getTables().keySet())) {
    		System.out.println(String.format("#%d\t%s", id, web.getTables().get(id).getPath()));
    	}
    	
		// using vote threshold
//    	runExperiment(new ValueBasedTableMatching(false, true, false, false, false, false, true, 1.0, true,2,0)); // itunes P72 R30 F43
//    	runExperiment(new ValueBasedTableMatching(false, true, false, false, false, false, true, 1.0, true,30,0)); // itunes P95 R6 F12
    	
    	// using matching value threshold:
//    	runExperiment(new ValueBasedTableMatching(false, false, false, false, false, false, false, 1.0, true,0,2)); // itunes P50 R66 F57
//    	runExperiment(new ValueBasedTableMatching(false, false, false, false, false, false, false, 1.0, true,0,10)); // itunes P46 R39
//    	runExperiment(new ValueBasedTableMatching(false, false, false, false, false, false, false, 1.0, true,0,131)); // itunes P86 R17
    	// with label refinement
//    	runExperiment(new ValueBasedTableMatching(false, false, false, false, false, false, false, 1.0, false,0,0));
//    	runExperiment(new ValueBasedTableMatching(false, false, false, false, false, false, false, 1.0, false,0,10)); // itunes P46 R39
//    	runExperiment(new ValueBasedTableMatching(false, false, true, true, false, false, false, 1.0, true,0,131)); // itunes P1 R48
//    	runExperiment(new ValueBasedTableMatching(false, false, true, true, false, false, false, 1.0, true,0,10)); // data.bls.gov perfect
    	
    	/********************************** 
		 * matchers basic, graph-refined, label-refined, full determinants
		 *********************************/
//    	// create one big cluster
//		runExperiment(new OneClusterMatcher(false, false));
//		// label-based
		runExperiment(new LabelBasedTableMatching(false, false, false, false, false, false));
//		// value-based
		runExperiment(new ValueBasedTableMatching(false, false, false, false, false, false, false, 1.0, true,0,0));
//		// entity-label duplicate-based
		runExperiment(new EntityLabelBasedMatching(false, false, false, false, false, false, 1.0,0));
//		// base: value refine: duplicate - link via determinants - no trusted keys
		runExperiment(new ValueBasedTableMatching(false, true, false, false, false, false, true, 1.0, true,0,0));
//		// base: value refine: duplicate - link via candidate keys - no trusted keys
		runExperiment(new ValueBasedTableMatching(false, true, false, false, false, false, false, 1.0, true,0,0));
//
//		// value-based +Graph
//		runExperiment(new ValueBasedTableMatching(false, false, false, true, false, false, false, 1.0, true,0,0));    	
//		// entity-label duplicate-based +Graph
//		runExperiment(new EntityLabelBasedMatching(false, false, false, true, false, false, 1.0,0));
//		// base: value refine: duplicate - link via determinants +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, true, false, false, true, 1.0, true,0,0));
//		// base: value refine: duplicate - link via candidate keys +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, true, false, false, false, 1.0, true,0,0));
//		
//		// value-based refine: label
//		runExperiment(new ValueBasedTableMatching(false, false, true, false, false, false, false, 1.0, true,0,0));
//		// entity-label duplicate-based refine: label
//		runExperiment(new EntityLabelBasedMatching(false, false, true, false, false, false, 1.0,0));
//		// base: value refine: duplicate & label - link via determinants - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, false, false, false, true, 1.0, true,0,0));
//		// base: value refine: duplicate & label - link via candidate keys - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, false, false, false, false, 1.0, true,0,0));
//		
//		
//		// value-based refine: label +Graph
		runExperiment(new ValueBasedTableMatching(false, false, true, true, false, false, false, 1.0, true,0,0));    	
//		// entity-label duplicate-based refine: label +Graph
		runExperiment(new EntityLabelBasedMatching(false, false, true, true, false, false, 1.0,0));
//		// base: value refine: duplicate & label - link via determinants +Graph - no trusted keys
		runExperiment(new ValueBasedTableMatching(false, true, true, true, false, false, true, 1.0, true,0,0));
//		// base: value refine: duplicate & label - link via candidate keys +Graph - no trusted keys
		runExperiment(new ValueBasedTableMatching(false, true, true, true, false, false, false, 1.0, true,0,0));

//    	/********************************** 
//		 * partial determinants
//		 *********************************/
//		// base: value refine: duplicate - link via determinants - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, false, false, false, true, 1.0, false,0,0));
//		// base: value refine: duplicate - link via candidate keys - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, false, false, false, false, 1.0, false,0,0));
//		// base: value refine: duplicate - link via determinants +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, true, false, false, true, 1.0, false,0,0));
//		// base: value refine: duplicate - link via candidate keys +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, true, false, false, false, 1.0, false,0,0));
//		// base: value refine: duplicate & label - link via determinants - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, false, false, false, true, 1.0, false,0,0));
//		// base: value refine: duplicate & label - link via candidate keys - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, false, false, false, false, 1.0, false,0,0));
//		// base: value refine: duplicate & label - link via determinants +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, true, false, false, true, 1.0, false,0,0));
//		// base: value refine: duplicate & label - link via candidate keys +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, true, false, false, false, 1.0, false,0,0));
    	
    	/********************************** 
		 * partial determinants, min 2 votes
		 *********************************/
//		// base: value refine: duplicate - link via determinants - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, false, false, false, true, 1.0, false,2,0));
//		// base: value refine: duplicate - link via candidate keys - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, false, false, false, false, 1.0, false,2,0));
//		// base: value refine: duplicate - link via determinants +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, true, false, false, true, 1.0, false,2,0));
//		// base: value refine: duplicate - link via candidate keys +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, false, true, false, false, false, 1.0, false,2,0));
//		// base: value refine: duplicate & label - link via determinants - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, false, false, false, true, 1.0, false,2,0));
//		// base: value refine: duplicate & label - link via candidate keys - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, false, false, false, false, 1.0, false,2,0));
//		// base: value refine: duplicate & label - link via determinants +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, true, false, false, true, 1.0, false,2,0));
//		// base: value refine: duplicate & label - link via candidate keys +Graph - no trusted keys
//		runExperiment(new ValueBasedTableMatching(false, true, true, true, false, false, false, 1.0, false,2,0));
	}

	protected void runExperiment(TableMatchingTask task) throws Exception {
		
		File resultsFile = new File(new File(resultLocation).getParentFile(), "experiments.csv");
		
		boolean isNew = !resultsFile.exists();
		
		CSVWriter w = new CSVWriter(new FileWriter(resultsFile, true));
		
		if(isNew) {
			w.writeNext(new String[] {
					"Time",
					"Duration",
					"Task Name",
					"Dataset Name",
					"Outcome",
					"Micro Avg Precision", 
					"Micro Avg Recall", 
					"Weighted Avg Precision", 
					"Weighted Avg Recall", 
					"Clustering Precision",
					"Penalised Clustering Precision",
					"Link-based Precision", 
					"Link-based Recall", 
					"Pair-wise Precision",
					"Pair-wise Recall",
					"Transitive Pair-wise Precision",
					"Transitive Pair-wise Recall",
					"Incorrect Correspondences",
					"Transitive Incorrect Correspondences",
					"Macro Avg Precision",
					"Macro Avg Recall",
					"Inverse Micro Avg Precision",
					"Inverse Micro Avg Recall",
					"Inverse Macro Avg Precision",
					"Inverse macro Avg Recall",
					"Message"
			});
		}
		
		String taskName = task.getTaskName();
		String dataName = new File(resultLocation).getName();
		
		System.out.println(String.format("######################## %s ########################", task.getClass().getName()));
		
//		try {
		
			task.setWebLocation(webLocation);
			task.setResultLocation(resultLocation);
			task.setWebTables(web);
			task.setMatchingEngine(matchingEngine);
			task.setDataProcessingEngine(proc);
			task.setDisjointHeaders(disjointHeaders);
			
			task.initialise();
			task.match();
		
			ClusteringPerformance performance = null;
//			System.out.println("Evaluation on original tables");
//			ClusteringPerformance performance = task.getSchemaPerformance();
//			System.out.println(performance.format(true));
			System.out.println("Evaluation on union tables");
			performance = task.getUnionSchemaPerformance();
			System.out.println(performance.format(false));
			
//			System.out.println("Evaluation on union tables (inverse)");
			ClusteringPerformance performanceInverse = task.getUnionSchemaPerformanceInverse();
//			System.out.println(performanceInverse.format(false));
			
			w.writeNext(new String[] { 
					DateTime.now().toString(),
					DurationFormatUtils.formatDurationHMS(task.getRuntime()),
					taskName,
					dataName,
					"success",
					Double.toString(performance.getMicroAverage().getPrecision()), 
					Double.toString(performance.getMicroAverage().getRecall()), 
					Double.toString(performance.getWeightedAverage().getPrecision()), 
					Double.toString(performance.getWeightedAverage().getRecall()), 
					Double.toString(performance.getClusteringPrecision()),
					Double.toString(performance.getPenalisedClusteringPrecision()),
					performance.getModelTheoreticPerformance()==null?"":Double.toString(performance.getModelTheoreticPerformance().getPrecision()), 
					performance.getModelTheoreticPerformance()==null?"":Double.toString(performance.getModelTheoreticPerformance().getRecall()), 
					performance.getCorrespondencePerformance()==null?"":Double.toString(performance.getCorrespondencePerformance().getPrecision()),
					performance.getCorrespondencePerformance()==null?"":Double.toString(performance.getCorrespondencePerformance().getRecall()),
					performance.getTransitiveCorrespondencePerformance()==null?"":Double.toString(performance.getTransitiveCorrespondencePerformance().getPrecision()),
					performance.getTransitiveCorrespondencePerformance()==null?"":Double.toString(performance.getTransitiveCorrespondencePerformance().getRecall()),
					Integer.toString(performance.getNumIncorrectCorrespondences()),
					Integer.toString(performance.getNumTransitiveIncorrectCorrespondences()),
					Double.toString(performance.getMacroAverage().getPrecision()),
					Double.toString(performance.getMacroAverage().getRecall()),
					Double.toString(performanceInverse.getMicroAverage().getPrecision()),
					Double.toString(performanceInverse.getMicroAverage().getRecall()),
					Double.toString(performanceInverse.getMacroAverage().getPrecision()),
					Double.toString(performanceInverse.getMacroAverage().getRecall()),
					
					""
				});
			
//		} catch(Exception ex) {
//			ex.printStackTrace();
//			
//			w.writeNext(new String[] {
//					DateTime.now().toString(),
//					"",
//					taskName,
//					dataName,
//					"exception",
//					"","",
//					"","",
//					"","",
//					"","",
//					"","",
//					"","",
//					"","",
//					"","",
//					ex.getMessage()
//			});
//		}
		
		w.close();
		
	}
}
