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
package de.uni_mannheim.informatik.dws.tnt.match.rules.refiner;

import java.util.Collection;
import java.util.LinkedList;

import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.MatchingRule;
import de.uni_mannheim.informatik.wdi.matrices.ArrayBasedSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultRecord;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Matches two records if the key values of one record can be found in the other record
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class RecordMatchingForSchemaVotingRule {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	

	public ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> run(ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences, DataProcessingEngine proc) {
	
		RecordMapper<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>> mapper = new RecordMapper<Correspondence<MatchableTableRow,MatchableTableColumn>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableColumn> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableColumn>> resultCollector) {
				
				Correspondence<MatchableTableRow, MatchableTableColumn> cor = apply(record);
				
				if(cor!=null) {
					resultCollector.next(cor);
				}
				
			}
		};
		
		return proc.transform(correspondences, mapper);
		
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.MatchingRule#apply(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	public Correspondence<MatchableTableRow, MatchableTableColumn> apply(Correspondence<MatchableTableRow, MatchableTableColumn> correspondence) {

		MatchableTableRow record1 = correspondence.getFirstRecord();
		MatchableTableRow record2 = correspondence.getSecondRecord();
		
		SimilarityMatrix<MatchableTableColumn> matchingValues = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(record1.getSchema().length, record2.getSchema().length);
		Collection<Pair<MatchableTableColumn, MatchableTableColumn>> matchingColumns = matchColumns(record1, record2, matchingValues);
		
		for(Pair<MatchableTableColumn, MatchableTableColumn> match : matchingColumns) {
			matchingValues.set(match.getFirst(), match.getSecond(), 1.0);
		}
		
		if(matchingValues.getNumberOfNonZeroElements()>0) {
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCors = new ResultSet<>();
			
			for(MatchableTableColumn col1 : matchingValues.getFirstDimension()) {
				for(MatchableTableColumn col2 : matchingValues.getMatches(col1)) {
					schemaCors.add(new Correspondence<MatchableTableColumn, MatchableTableRow>(col1, col2, 1.0, null));
				}
			}
			
			//TODO this matcher breaks the correspondence lineage, it should rather return the votes (schema cors) with their lineage than the input (instance cors) as groups of schema cors 
			return new Correspondence<MatchableTableRow, MatchableTableColumn>(record1, record2, correspondence.getSimilarityScore(), schemaCors);
		} else {
			return null;
		}
	}
		
	public Collection<Pair<MatchableTableColumn, MatchableTableColumn>> matchColumns(MatchableTableRow record1, MatchableTableRow record2, SimilarityMatrix<MatchableTableColumn> matchingValues) {
		
		Collection<Pair<MatchableTableColumn, MatchableTableColumn>> matchingColumns = new LinkedList<>();

			for(MatchableTableColumn col : record1.getSchema()) {
				
				if(!SpecialColumns.isSpecialColumn(col)) {
				
					Object value = record1.get(col.getColumnIndex());
					
					if(value==null) {
						value = "null";
					}
					
					for(MatchableTableColumn col2 : record2.getSchema()) {
						
						if(!SpecialColumns.isSpecialColumn(col2)) {
							Object value2 = record2.get(col2.getColumnIndex());
							
							if(value2==null) {
								value2 = "null";
							}
							
							if(value.equals(value2)) {
								matchingColumns.add(new Pair<MatchableTableColumn, MatchableTableColumn>(col, col2));
							}
						}
					}
				}
			}
		
		return matchingColumns;
	}


}
