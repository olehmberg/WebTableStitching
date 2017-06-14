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
package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;

import com.beust.jcommander.Parameter;

import au.com.bytecode.opencsv.CSVReader;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.io.CSVCorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * 
 * Transforms the correspondences in the merged table(s) to correspondences on the original tables and evaluates them
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EvaluateMergedCorrespondences extends Executable {

	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	@Parameter(names = "-correspondences", required=true)
	private String correspondencesLocation;
	
	@Parameter(names = "-transformedCorrespondences")
	private String transformedLocation;
	
	@Parameter(names = "-gs")
	private String gsLocation;
	
	@Parameter(names = "-verbose")
	private boolean verbose = false;
	
	public static void main(String[] args) throws IOException {
		EvaluateMergedCorrespondences eval = new EvaluateMergedCorrespondences();
		
		if(eval.parseCommandLine(EvaluateMergedCorrespondences.class, args)) {
			eval.run();
		}
	}
	
	public void run() throws IOException {
		File webFile = new File(webLocation);
		WebTables web = WebTables.loadWebTables(webFile, true, false, false, false);
		
		File corFile = new File(correspondencesLocation);
		
		MatchingGoldStandard gs = null;
		if(gsLocation!=null) {
			File gsFile = new File(gsLocation);
			
			gs = new MatchingGoldStandard();
			gs.setComplete(true);
			gs.loadFromCSVFile(gsFile);
			
//			Performance perf = evaluator.evaluateMatching(result.get(), gs);
//			
//			System.out
//			.println(String.format(
//					"Schema Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
//					perf.getPrecision(), perf.getRecall(),
//					perf.getF1()));
		}
		
		CSVReader r = new CSVReader(new FileReader(corFile));
		String[] values = null;
		
		Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> result = new ProcessableCollection<>(); 
		
		while((values = r.readNext())!=null) {
			
			System.out.println(StringUtils.join(values,","));
			
			String webId = values[0];
			String kbId = values[1];
			Double sim = Double.parseDouble(values[2]);
			
			MatchableTableColumn webMC = web.getSchema().getRecord(webId);
			TableColumn webCol = web.getTables().get(webMC.getTableId()).getSchema().get(webMC.getColumnIndex());
			MatchableTableColumn kbMC = new MatchableTableColumn(kbId);
			
			// exclude context columns (added by tnt) as they are not included in the gold standard
			if(!ContextColumns.isContextColumn(webCol)) {

				if(gs!=null) {
					System.out.println(String.format("%s <-> %s == %s (%d correspondences)", webMC.getIdentifier(), kbMC.getIdentifier(), gs.containsPositive(webId, kbId), webCol.getProvenance().size()));
				}
				
				for(String prov : new HashSet<>(webCol.getProvenance())) {
					String[] provValues = prov.split(";");
					
					String id = String.format("%s~Col%s", provValues[0], provValues[1]);
					
					MatchableTableColumn originalWebMC = new MatchableTableColumn(id);
					
					Correspondence<MatchableTableColumn, MatchableTableColumn> cor = new Correspondence<MatchableTableColumn, MatchableTableColumn>(originalWebMC, kbMC, sim, null);
					result.add(cor);
				}
			}
			
		}
		
		r.close();
		
		result = result.distinct();
		System.out.println(String.format("%d schema correspondences", result.size()));
		
		if(gs!=null) {
			MatchingEvaluator<MatchableTableColumn, MatchableTableColumn> evaluator = new MatchingEvaluator<>(verbose);
			
			Performance perf = evaluator.evaluateMatching(result.get(), gs);
			
			System.out
			.println(String.format(
					"Schema Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
					perf.getPrecision(), perf.getRecall(),
					perf.getF1()));
		}
		
		if(transformedLocation!=null) {
			File transformedFile = new File(transformedLocation);
			new CSVCorrespondenceFormatter().writeCSV(transformedFile, result);
		}
		

	}
}
