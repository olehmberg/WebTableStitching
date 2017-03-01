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
import java.util.Map;
import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandardCreator;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.Performance;
import de.uni_mannheim.informatik.wdi.parallel.ParallelDataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class Evaluate extends TnTTask {

	@Parameter(names = "-web")
	private String webLocation;

	@Parameter(names = "-correspondences")
	private String correspondenceLocation;

	/**
	 * @param correspondenceLocation the correspondenceLocation to set
	 */
	public void setCorrespondenceLocation(String correspondenceLocation) {
		this.correspondenceLocation = correspondenceLocation;
	}
	
	
	public static void main(String[] args) throws Exception {
		Evaluate e = new Evaluate();

		if (e.parseCommandLine(Evaluate.class, args)) {

			hello();

			e.initialise();
			e.setDataProcessingEngine(new ParallelDataProcessingEngine());
			e.setMatchingEngine(new MatchingEngine<MatchableTableRow, MatchableTableColumn>());

			e.match();

		}
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.TnTTask#initialise()
	 */
	@Override
	public void initialise() throws Exception {
		// create a gold standard from a manual mapping of the union tables
		
		File webFile = new File(webLocation);
		
		if(webFile.exists()) {
			// load web tables
			WebTables web = WebTables.loadWebTables(webFile, true, false, false, true);
	    	// prepare gold standard
	    	N2NGoldStandardCreator gsc = new N2NGoldStandardCreator();
	    	gsc.createFromMappedUnionTables(web.getTables().values(), new File(correspondenceLocation));
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.TnTMatch#match()
	 */
	@Override
	public void match() throws Exception {
		File corLoc = new File(correspondenceLocation);
		
		N2NGoldStandard correspondencesC1 = new N2NGoldStandard();
		N2NGoldStandard correspondencesC3 = new N2NGoldStandard();
		System.out.println("Loading correspondences ...");
		correspondencesC1.loadFromTSV(new File(corLoc, "correspondences_union.tsv"));
		correspondencesC3.loadFromTSV(new File(corLoc, "correspondences_join_union.tsv"));
		
    	N2NGoldStandard gs = new N2NGoldStandard();
    	System.out.println("Loading gold standard ...");
    	gs.loadFromTSV(new File(corLoc, "goldstandard.tsv"));

    	/******************************************************
    	 * Eval Union
    	 ******************************************************/
    	
    	
    	printHeadline("Evaluating columns (C1) in gold standard ...");
    	ClusteringPerformance perf = gs.evaluateCorrespondenceClusters(correspondencesC1.getCorrespondenceClusters(), true);
    	System.out.println(gs.formatEvaluationResult(perf.getPerformanceByCluster(), false));
    	
//    	System.out.println("*** Evaluation C1 ***");
//    	for(String key : perf.keySet()) {
//    		Performance p = perf.get(key);
//    		System.out.println(String.format("%s:\tprec: %.4f\trec:%.4f\tf1:%.4f\t\t%d/%d/%d", org.apache.commons.lang3.StringUtils.rightPad(key, 30), p.getPrecision(), p.getRecall(), p.getF1(), p.getNumberOfPredicted(), p.getNumberOfCorrectlyPredicted(), p.getNumberOfCorrectTotal()));
//    	}
    	
    	/******************************************************
    	 * Eval Join Union
    	 ******************************************************/
    	
    	printHeadline("Evaluating created columns (C3) ...");
    	System.out.println("Evaluating created columns (C3) ...");
    	perf = gs.evaluateCorrespondenceClustersInverse(correspondencesC3.getCorrespondenceClusters().keySet(), false);
    	System.out.println(gs.formatEvaluationResult(perf.getPerformanceByCluster(), false));
//    	System.out.println("*** Evaluation ***");
//    	for(String key : perf.keySet()) {
//    		Performance p = perf.get(key);
//    		System.out.println(String.format("%s:\tprec: %.4f\trec:%.4f\tf1:%.4f\t\t%d/%d/%d", org.apache.commons.lang3.StringUtils.rightPad(key, 30), p.getPrecision(), p.getRecall(), p.getF1(), p.getNumberOfPredicted(), p.getNumberOfCorrectlyPredicted(), p.getNumberOfCorrectTotal()));
//    	}
    	
    	printHeadline("Evaluating columns (C3) in gold standard ...");
    	perf = gs.evaluateCorrespondenceClusters(correspondencesC3.getCorrespondenceClusters(), true);
    	System.out.println(gs.formatEvaluationResult(perf.getPerformanceByCluster(), false));
    	
//    	System.out.println("*** Evaluation C3 ***");
//    	for(String key : perf.keySet()) {
//    		Performance p = perf.get(key);
//    		System.out.println(String.format("%s:\tprec: %.4f\trec:%.4f\tf1:%.4f\t\t%d/%d/%d", org.apache.commons.lang3.StringUtils.rightPad(key, 30), p.getPrecision(), p.getRecall(), p.getF1(), p.getNumberOfPredicted(), p.getNumberOfCorrectlyPredicted(), p.getNumberOfCorrectTotal()));
//    	}
	}

}
