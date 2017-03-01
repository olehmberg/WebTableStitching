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
package de.uni_mannheim.informatik.dws.t2k.fuse;

//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.Map;
//
//import org.joda.time.DateTime;
//
//import com.beust.jcommander.Parameter;
//
//import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
//import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
//import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
//import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRowFactory;
//import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRowGroupFactory;
//import de.uni_mannheim.informatik.dws.t2k.match.data.TableCorrespondenceParser;
//import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.utils.cli.Executable;
//import de.uni_mannheim.informatik.wdi.datafusion.CorrespondenceSet;
//import de.uni_mannheim.informatik.wdi.datafusion.DataFusionEngine;
//import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.Voting;
//import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.numeric.Median;
//import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.string.LongestString;
//import de.uni_mannheim.informatik.wdi.model.Correspondence;
//import de.uni_mannheim.informatik.wdi.model.ResultSet;
//import de.uni_mannheim.informatik.wdi.parallel.ParallelDataFusionEngine;
//import de.uni_mannheim.informatik.wdi.similarity.date.WeightedDateSimilarity;
//import de.uni_mannheim.informatik.wdi.similarity.modifiers.QuadraticSimilarityMeasureModifier;
//import de.uni_mannheim.informatik.wdi.similarity.numeric.UnadjustedDeviationSimilarity;
//import de.uni_mannheim.informatik.wdi.similarity.string.GeneralisedStringJaccard;
//import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ConsistencyReport extends Executable {
//
//	@Parameter(names = "-instanceCorrespondences")
//	private String instanceCorrespondenceFile;
//	
//	@Parameter(names = "-schemaCorrespondences")
//	private String schemaCorrespondenceFile;
//	
//	@Parameter(names = "-web")
//	private String webTables;
//	
//	@Parameter(names = "-kb")
//	private String knowledgeBase;
//	
//	@Parameter(names = "-out")
//	private String outputFileLocation;
//	
//	public static void main(String[] args) throws IOException {
//		ConsistencyReport cr = new ConsistencyReport();
//		
//		if(cr.parseCommandLine(ConsistencyReport.class, args)) {
//			
//			cr.run();
//			
//		}
//	}
//
//	private void run() throws IOException {
//		// load datasets, use WebTables class, add serialisation
//		// data model requirements: 
//		// - value by row, row connected to instance correspondence
//		// - value by column, column connected to schema correspondence
//		// - value by PLD, for intra-/inter-PLD consistency = provenance, add ProvenanceId to MatchableTableRow
//		WebTables web = WebTables.loadWebTables(new File(webTables), false, true);
//		
//		//TODO implement batch processing to avoid loading 1 million web tables at once
//		
//		// load correspondences
//		// create from db, load from single file
//		CorrespondenceSet<MatchableTableRow, MatchableTableColumn> instanceCorrespondences = new CorrespondenceSet<>();
//		instanceCorrespondences.setGroupFactory(new MatchableTableRowGroupFactory());
//		instanceCorrespondences.loadCorrespondences(new File(instanceCorrespondenceFile), web.getRecords());
//		
//		System.out.println(String.format("%,d record groups", instanceCorrespondences.getRecordGroups().size()));
//		
//		// create from db, load from single file
//		TableCorrespondenceParser p = new TableCorrespondenceParser();
//		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = p.loadCorrespondencesFromCSV(new File(schemaCorrespondenceFile), web.getTableIndices());
//		
//		// calculate consistencies
//		MatchableTableRowFactory factory = new MatchableTableRowFactory();
//		DataTypeDependentDataFusionStrategy strategy = new DataTypeDependentDataFusionStrategy(factory);
//		strategy.addFuserForDataType(
//				DataType.string, 
//				new MatchableTableRowFuser<>(new LongestString<MatchableTableRow, MatchableTableColumn>()), 
//				new MatchableTableRowEvaluationRule<>(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), 0.5));
//		strategy.addFuserForDataType(
//				DataType.numeric, 
//				new MatchableTableRowFuser<>(new Median<MatchableTableRow, MatchableTableColumn>()), 
//				new MatchableTableRowEvaluationRule<>(new QuadraticSimilarityMeasureModifier<>(new UnadjustedDeviationSimilarity()), 0.8));
//		strategy.addFuserForDataType(
//				DataType.date, 
//				new MatchableTableRowFuser<>(new Voting<DateTime, MatchableTableRow, MatchableTableColumn>()), 
//				new MatchableTableRowEvaluationRule<>(new WeightedDateSimilarity(1, 3, 5) , 0.8));
//		
////		DataFusionEngine<MatchableTableRow, MatchableTableColumn> engine = new DataFusionEngine<>(strategy);
//		DataFusionEngine<MatchableTableRow, MatchableTableColumn> engine = new ParallelDataFusionEngine<>(strategy);
//		
//		System.out.println("Calculating consistencies ...");
//		Map<String, Double> consistencies = engine.getAttributeConsistencies(instanceCorrespondences, schemaCorrespondences);
//		
//		System.out.println("Writing consistencies ...");
//		BufferedWriter w = new BufferedWriter(new FileWriter(new File(outputFileLocation)));
//		// write consistencies
//		for(String key : consistencies.keySet()) {
//			if(consistencies.get(key)>1.0) {
//				System.out.println("Wrong Consistency!");
//			}
//			String line = String.format("%s\t%s", key, Double.toString(consistencies.get(key)));
////			System.out.println(line);
//			w.write(line + "\n");
//		}
//		
//		w.close();
//		
//		System.out.println("done.");
//	}
	
}
