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
package de.uni_mannheim.informatik.dws.t2k.match;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.CorrespondenceWeightingRecordMapper;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.GroupCorrespondencesRecordKeyMapper;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.SumCorrespondenceSimilarityAggregator;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CombineSchemaCorrespondences {

	private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
	private ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences;
	
	private ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences;
	/**
	 * @param schemaCorrespondences the schemaCorrespondences to set
	 */
	public void setSchemaCorrespondences(
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		this.schemaCorrespondences = schemaCorrespondences;
	}
	
	private ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> labelBasedSchemaCorrespondences;
	/**
	 * @param labelBasedSchemaCorrespondences the labelBasedSchemaCorrespondences to set
	 */
	public void setLabelBasedSchemaCorrespondences(
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> labelBasedSchemaCorrespondences) {
		this.labelBasedSchemaCorrespondences = labelBasedSchemaCorrespondences;
	}
	
	public CombineSchemaCorrespondences(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences) {
		this.matchingEngine = matchingEngine;
		this.keyCorrespondences = keyCorrespondences;
	}
	
	private boolean verbose = false;
	/**
	 * @param verbose the verbose to set
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	/**
	 * @return the verbose
	 */
	public boolean isVerbose() {
		return verbose;
	}
	
	private double finalPropertySimilarityThreshold = 0.03;	
	
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> run() {
		DataProcessingEngine dataEngine = matchingEngine.getProcessingEngine();
    	
		// multiply the similarity scores with the respective weights (duplicate: 0.8; label: 0.2)
    	CorrespondenceWeightingRecordMapper weightDuplicateBased = new CorrespondenceWeightingRecordMapper(0.8);
    	schemaCorrespondences = dataEngine.transform(schemaCorrespondences, weightDuplicateBased);
    	
    	CorrespondenceWeightingRecordMapper weightLabelBased = new CorrespondenceWeightingRecordMapper(0.2);
    	labelBasedSchemaCorrespondences = dataEngine.transform(labelBasedSchemaCorrespondences, weightLabelBased);
    	
    	// combine the correspondences
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> combinresult = dataEngine.append(schemaCorrespondences, labelBasedSchemaCorrespondences);
    	
    	// group correspondences between the same column/property combination by summing up the weighted scores
    	GroupCorrespondencesRecordKeyMapper groupCorrespondences = new GroupCorrespondencesRecordKeyMapper();
    	SumCorrespondenceSimilarityAggregator sumSimilarity = new SumCorrespondenceSimilarityAggregator();
    	ResultSet<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> sum = dataEngine.aggregateRecords(combinresult, groupCorrespondences, sumSimilarity);
    	
//    	AggregateToValueRecordMapper<String, Correspondence<MatchableTableColumn, MatchableTableRow>> sumToCorrespondences = new AggregateToValueRecordMapper<>();
    	
    	if(isVerbose()) {
    		for(Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>> record : sum.get()) {
    			System.out.println(String.format("[%b] (%.8f) %s <-> %s", record.getSecond().getSimilarityScore()>=finalPropertySimilarityThreshold, record.getSecond().getSimilarityScore(), record.getSecond().getFirstRecord(), record.getSecond().getSecondRecord()));
    		}
    	}
    	
    	RecordMapper<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn, MatchableTableRow>> sumToCorrespondences = new RecordMapper<Pair<String,Correspondence<MatchableTableColumn,MatchableTableRow>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
				if(record.getSecond().getSimilarityScore()>=finalPropertySimilarityThreshold) {
					resultCollector.next(record.getSecond());
				}
			}
		};
		schemaCorrespondences = dataEngine.transform(sum, sumToCorrespondences);
			    	
    	//TODO it's possible that the entitylabel column is also mapped to a property other than rdfs:Label (2 correspondences) (why?) - that should not happen
    	schemaCorrespondences = dataEngine.append(schemaCorrespondences, keyCorrespondences);
    	
    	System.out.println(String.format("%d schema correspondences (including %d key correspondences)", schemaCorrespondences.size(), keyCorrespondences.size()));
    	
    	return schemaCorrespondences;
	}
	
}
