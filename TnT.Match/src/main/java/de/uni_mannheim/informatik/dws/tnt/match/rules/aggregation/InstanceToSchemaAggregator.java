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
package de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation;

import java.util.Collection;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Aggregates instance correspondences per table to schema correspondences by using the causal correspondences as votes
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class InstanceToSchemaAggregator {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> aggregate(ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences,
			DataProcessingEngine proc) {
		
		RecordKeyValueMapper<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>> groupByTablesMapper = new RecordKeyValueMapper<Collection<Integer>, Correspondence<MatchableTableRow,MatchableTableColumn>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableColumn> record,
					DatasetIterator<Pair<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {
				
				if(record.getFirstRecord().getTableId()>record.getSecondRecord().getTableId()) {
					System.out.println("Wrong direction!");
				}
				
				resultCollector.next(new Pair<Collection<Integer>, Correspondence<MatchableTableRow,MatchableTableColumn>>(Q.toList(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
			}
		};
		
		// group by table
		ResultSet<Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableColumn>>> grouped = proc.groupRecords(correspondences, groupByTablesMapper);
		
		
		// aggregate votes
		RecordMapper<Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableColumn>>, Correspondence<MatchableTableColumn, MatchableTableRow>> voteMapper = new RecordMapper<Group<Collection<Integer>,Correspondence<MatchableTableRow,MatchableTableColumn>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableColumn>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
		
				SimilarityMatrix<MatchableTableColumn> m = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
				
				// each instance correspondence votes
				for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : record.getRecords().get()) {
					
					// for every applicable schema correspondence
					for(Correspondence<MatchableTableColumn, MatchableTableRow> schemaCor : cor.getCausalCorrespondences().get()) {
						
						m.add(schemaCor.getFirstRecord(), schemaCor.getSecondRecord(), schemaCor.getSimilarityScore());
						
					}
					
				}
				
				// based on the votes, decide on a final schema mapping
				BestChoiceMatching bcm = new BestChoiceMatching();
				m = bcm.match(m);
				
				//m.normalize();
				m.normalize(record.getRecords().size());
				
				// and output the correspondences
				for(MatchableTableColumn c1 : m.getFirstDimension()) {
					for(MatchableTableColumn c2 : m.getMatches(c1)) {
						
						if(m.get(c1, c2)==1.0) {
							// create the schema correspondence
							Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, m.get(c1, c2), record.getRecords());
							
							resultCollector.next(cor);
						} else {
//							System.out.println(String.format("* uncertain: %s<->%s (%.6f)", c1, c2, m.get(c1, c2)));
						}
					}
				}
			}
		};
		
		
		return proc.transform(grouped, voteMapper);
		
	}
	
}
