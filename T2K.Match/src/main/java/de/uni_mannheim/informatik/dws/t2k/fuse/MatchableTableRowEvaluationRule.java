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

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.datafusion.EvaluationRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowEvaluationRule<ValueType> extends EvaluationRule<MatchableTableRow, MatchableTableColumn> {

	private SimilarityMeasure<ValueType> measure;
	private double threshold;
	
	public MatchableTableRowEvaluationRule() {
	}
	
	public MatchableTableRowEvaluationRule(SimilarityMeasure<ValueType> measure, double threshold) {
		this.measure = measure;
		this.threshold = threshold;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.EvaluationRule#isEqual(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean isEqual(MatchableTableRow record1,
			MatchableTableRow record2, MatchableTableColumn schemaElement) {
		return measure.calculate((ValueType)record1.get(schemaElement.getColumnIndex()), (ValueType)record2.get(schemaElement.getColumnIndex())) >= threshold;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.EvaluationRule#isEqual(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean isEqual(MatchableTableRow record1,
			MatchableTableRow record2,
			Correspondence<MatchableTableColumn, MatchableTableRow> schemaCorrespondence) {
		
		double sim = 0.0;
		
		int c1 = schemaCorrespondence.getFirstRecord().getColumnIndex();
		int c2 = schemaCorrespondence.getSecondRecord().getColumnIndex();
		
		if(record1.getType(c1)==record2.getType(c2)) { // problem: property correspondences in .csv don't make sense ...
			try {
				ValueType v1 = (ValueType)record1.get(c1);
				
				ValueType v2 = (ValueType)record2.get(c2);
				sim = measure.calculate(v1, v2);
			} catch(Exception e) {
				//System.out.println(String.format("Error converting value '%s' [%s] or value '%s' [%s] for correspondence %s -> %s using records %s & %s", record1.get(c1), record1.getType(c1), record2.get(c2), record2.getType(c2), schemaCorrespondence.getFirstRecord().getIdentifier(), schemaCorrespondence.getSecondRecord().getIdentifier(), record1.getIdentifier(), record2.getIdentifier()));
			}
		}
		
		return sim >= threshold;
	}
	

}
