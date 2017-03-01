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
 * exactly the same code as InstanceToSchemaAggregator but with MatchableTableRow and MatchableTableColumn switched
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaToInstanceAggregator {

	public ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> aggregate(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences,
			DataProcessingEngine proc) {
		
		RecordKeyValueMapper<Set<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>> groupByTablesMapper = new RecordKeyValueMapper<Set<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableColumn, MatchableTableRow> record,
					DatasetIterator<Pair<Set<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {
				resultCollector.next(new Pair<Set<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>(Q.toSet(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
			}
		};
		
		// group by table
		ResultSet<Group<Set<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>> grouped = proc.groupRecords(correspondences, groupByTablesMapper);
		
		
		// aggregate votes
		RecordMapper<Group<Set<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableRow, MatchableTableColumn>> voteMapper = new RecordMapper<Group<Set<Integer>,Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Set<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableColumn>> resultCollector) {
		
				SimilarityMatrix<MatchableTableRow> m = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
				
				// each instance correspondence votes
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : record.getRecords().get()) {
					
					// for every applicable schema correspondence
					for(Correspondence<MatchableTableRow, MatchableTableColumn> schemaCor : cor.getCausalCorrespondences().get()) {
						
						Double sum = m.get(schemaCor.getFirstRecord(), schemaCor.getSecondRecord());
						if(sum==null) {
							sum = 0.0;
						}
						
						sum += schemaCor.getSimilarityScore();
						
						m.set(schemaCor.getFirstRecord(), schemaCor.getSecondRecord(), sum);
						
					}
					
				}
				
				// based on the votes, decide on a final schema mapping
				BestChoiceMatching bcm = new BestChoiceMatching();
				m = bcm.match(m);
				
				// and output the correspondences
				for(MatchableTableRow c1 : m.getFirstDimension()) {
					for(MatchableTableRow c2 : m.getMatches(c1)) {
						
						// create the schema correspondence
						Correspondence<MatchableTableRow, MatchableTableColumn> cor = new Correspondence<MatchableTableRow, MatchableTableColumn>(c1, c2, m.get(c1, c2), record.getRecords());
						
						resultCollector.next(cor);
					}
				}
			}
		};
		
		
		return proc.transform(grouped, voteMapper);
		
	}
	
}
