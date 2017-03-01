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

import java.util.List;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class InstanceTransitivityAggregator {

	public ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> aggregate(ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences,
			DataProcessingEngine proc) {
		
		// join correspondences A=(a,b) and B=(b,c) on A.b=B.b
		
		// create join key from A.b
		Function<List<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>> joinKeyGeneratorA = new Function<List<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public List<Integer> execute(Correspondence<MatchableTableRow, MatchableTableKey> input) {
				int tableId = input.getSecondRecord().getTableId();
				int row = input.getSecondRecord().getRowNumber();
				
				return Q.toList(tableId, row);
			}
		};
		
		// create join key from B.b
		Function<List<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>> joinKeyGeneratorB = new Function<List<Integer>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public List<Integer> execute(Correspondence<MatchableTableRow, MatchableTableKey> input) {
				int tableId = input.getFirstRecord().getTableId();
				int row = input.getFirstRecord().getRowNumber();
				
				return Q.toList(tableId, row);
			}
		};
		
		ResultSet<Pair<Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>>> join1 = proc.join(correspondences, correspondences, joinKeyGeneratorA, joinKeyGeneratorB);
		
		// join merged correspondences AB=(a,b,b,c) with C=(c,a)
		
		// create join key from AB.a and AB.c
		Function<List<Integer>, Pair<Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>>> joinKeyGeneratorAB = new Function<List<Integer>, Pair<Correspondence<MatchableTableRow,MatchableTableKey>,Correspondence<MatchableTableRow,MatchableTableKey>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public List<Integer> execute(
					Pair<Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>> input) {
				int table1Id = input.getFirst().getFirstRecord().getTableId();
				int row1 = input.getFirst().getFirstRecord().getRowNumber();
				int table2Id = input.getSecond().getSecondRecord().getTableId();
				int row2 = input.getSecond().getSecondRecord().getRowNumber();
				
				return Q.toList(table1Id, row1, table2Id, row2);
			}
		};
		
		// create join key from C.c and C.a
		Function<List<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>> joinKeyGeneratorC = new Function<List<Integer>, Correspondence<MatchableTableRow,MatchableTableKey>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public List<Integer> execute(Correspondence<MatchableTableRow, MatchableTableKey> input) {
				int table1Id = input.getFirstRecord().getTableId();
				int row1 = input.getFirstRecord().getRowNumber();
				int table2Id = input.getSecondRecord().getTableId();
				int row2 = input.getSecondRecord().getRowNumber();
				
				return Q.toList(table1Id, row1, table2Id, row2);
			}
		};
		
		ResultSet<Pair<Pair<Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableRow, MatchableTableKey>>> join2 = proc.leftJoin(join1, correspondences, joinKeyGeneratorAB, joinKeyGeneratorC);
		
		// if the correspondence C=(c,a) is missing, create it
		RecordMapper<Pair<Pair<Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableRow, MatchableTableKey>> joinsToMissingCorrespondenceMapper = new RecordMapper<Pair<Pair<Correspondence<MatchableTableRow,MatchableTableKey>,Correspondence<MatchableTableRow,MatchableTableKey>>,Correspondence<MatchableTableRow,MatchableTableKey>>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Pair<Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableRow, MatchableTableKey>> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableKey>> resultCollector) {
				
				if(record.getSecond()==null) {
					
					if(record.getFirst().getFirst().getFirstRecord().getTableId()>record.getFirst().getSecond().getSecondRecord().getTableId()) {
						System.out.println("Wrong direction");
					}
					
					// the transitive correspondence does not exist in the original data
					// so we create it
					Correspondence<MatchableTableRow, MatchableTableKey> transitive = 
							new Correspondence<MatchableTableRow, MatchableTableKey>(record.getFirst().getFirst().getFirstRecord(), record.getFirst().getSecond().getSecondRecord(), 1.0, null);
					resultCollector.next(transitive);
				}
				
			}
		};
		
		return proc.transform(join2, joinsToMissingCorrespondenceMapper);
	}
	
}
