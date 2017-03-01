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
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.AggregateToValueRecordMapper;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.CorrespondenceWeightingRecordMapper;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.GroupCorrespondencesRecordKeyMapper;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.SumCorrespondenceSimilarityAggregator;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class UpdateSchemaCorrespondences {

	private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
	
	private ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences;
	/**
	 * @param schemaCorrespondences the schemaCorrespondences to set
	 */
	public void setSchemaCorrespondences(
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		this.schemaCorrespondences = schemaCorrespondences;
	}
	
	private ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> newSchemaCorrespondences;
	/**
	 * @param labelBasedSchemaCorrespondences the labelBasedSchemaCorrespondences to set
	 */
	public void setNewSchemaCorrespondences(
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> newSchemaCorrespondences) {
		this.newSchemaCorrespondences = newSchemaCorrespondences;
	}
	
	public UpdateSchemaCorrespondences(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine) {
		this.matchingEngine = matchingEngine;
	}
	
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> run() {
		DataProcessingEngine dataEngine = matchingEngine.getProcessingEngine();
    	
		// multiply the similarity scores with the respective weights (old: 0.5; new: 0.5)
    	CorrespondenceWeightingRecordMapper weightDuplicateBased = new CorrespondenceWeightingRecordMapper(0.5);
    	schemaCorrespondences = dataEngine.transform(schemaCorrespondences, weightDuplicateBased);
    	
    	CorrespondenceWeightingRecordMapper weightLabelBased = new CorrespondenceWeightingRecordMapper(0.5);
    	newSchemaCorrespondences = dataEngine.transform(newSchemaCorrespondences, weightLabelBased);
    	
    	// combine the correspondences
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> combinresult = dataEngine.append(schemaCorrespondences, newSchemaCorrespondences);
    	
    	// group correspondences between the same column/property combination by summing up the weighted scores
    	GroupCorrespondencesRecordKeyMapper groupCorrespondences = new GroupCorrespondencesRecordKeyMapper();
    	SumCorrespondenceSimilarityAggregator sumSimilarity = new SumCorrespondenceSimilarityAggregator();
    	ResultSet<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> sum = dataEngine.aggregateRecords(combinresult, groupCorrespondences, sumSimilarity);
    	
    	AggregateToValueRecordMapper<String, Correspondence<MatchableTableColumn, MatchableTableRow>> sumToCorrespondences = new AggregateToValueRecordMapper<>();
    	return dataEngine.transform(sum, sumToCorrespondences);
	}
}
