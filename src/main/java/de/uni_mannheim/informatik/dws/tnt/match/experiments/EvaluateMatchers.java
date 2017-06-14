package de.uni_mannheim.informatik.dws.tnt.match.experiments;

import java.io.File;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.TableToTableEvaluator;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.CandidateKeyBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.DeterminantBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.EntityLabelBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.LabelBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.TableToTableMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.ValueBasedMatcher;

public class EvaluateMatchers extends TnTTask {

	@Parameter(names = "-web", required=true)
	private String webLocation;

	@Parameter(names = "-eval")
	private String evaluationLocation;
	
	public static void main(String[] args) throws Exception {
		EvaluateMatchers app = new EvaluateMatchers();
		if(app.parseCommandLine(EvaluateMatchers.class, args)) {
			TnTTask.hello();
			app.initialise();
			app.match();
		}
	}
	
	protected WebTables web;
	protected N2NGoldStandard gs;
	protected N2NGoldStandard unionGs;
	protected DisjointHeaders disjointHeaders;
	
	@Override
	public void initialise() throws Exception {
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, false);
		
		disjointHeaders = DisjointHeaders.fromTables(web.getTables().values());
		
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

	@Override
	public void match() throws Exception {
		
		TableToTableMatcher[] matchers = new TableToTableMatcher[] {
			new ValueBasedMatcher(),
			new EntityLabelBasedMatcher(),
			new DeterminantBasedMatcher(),
			new CandidateKeyBasedMatcher(),
			new LabelBasedMatcher()
		};
		
		TableToTableEvaluator eval = new TableToTableEvaluator();
		
		File evaluationFile = new File(evaluationLocation);
		
		for(TableToTableMatcher m : matchers) {
			
			m.setWebTables(web);
			m.setDisjointHeaders(disjointHeaders.getAllDisjointHeaders());
			
			m.initialise();
			m.match();
			
			ClusteringPerformance performance = eval.evaluateSchemaCorrespondences(m.getSchemaCorrespondences(), web, evaluationFile);
			
			System.out.println(String.format("%s: \n\tPrecision: %f\n\tRecall: %f\n\tF1-Measure: %f", 
					m.getClass().getName(),
					performance.getCorrespondencePerformance().getPrecision(),
					performance.getCorrespondencePerformance().getRecall(),
					performance.getCorrespondencePerformance().getF1()));
			
		}
	}

}
