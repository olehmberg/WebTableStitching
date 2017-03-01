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
package de.uni_mannheim.informatik.dws.tnt.match.rules.blocking;

import java.util.Arrays;
import java.util.List;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.InstanceCorrespondence;
import de.uni_mannheim.informatik.dws.tnt.match.data.KeyCorrespondence;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
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
public class MatchingKeyRecordBlockerImproved {

	public ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> runBlocking(
			DataSet<MatchableTableRow, MatchableTableColumn> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> schemaCorrespondences,
			DataProcessingEngine engine) {
	
		//TODO this blocker seems to be too expensive, change computation to aggregation instead of join (group by key values and directly aggregate?)
		
		System.out.println("MatchingKeyRecordBlocker");
		System.out.println(String.format("%d key correspondences", schemaCorrespondences.size()));
		
		// join all rows via their table id to matching keys (multiple keys per rows)
		Function<Integer, MatchableTableRow> rowToTableId = new Function<Integer, MatchableTableRow>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableRow input) {
				return input.getTableId();
			}
		};
		Function<Integer, Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorToLeftTableId = new Function<Integer, Correspondence<MatchableTableKey, MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableKey, MatchableTableColumn> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		
		// JOIN row+key via tableId (left)
		ResultSet<Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> rowsWithLeftKeys = engine.joinMixedTypes(dataset, schemaCorrespondences, rowToTableId, keyCorToLeftTableId);
		System.out.println(String.format("left key join: %d records", rowsWithLeftKeys.size()));
		
		// also genenrate the join key values for the right-hand side
		Function<Integer, Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorToRightTableId = new Function<Integer, Correspondence<MatchableTableKey, MatchableTableColumn>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableKey, MatchableTableColumn> input) {
				return input.getSecondRecord().getTableId();
			}
		};
		
		// JOIN row+key via tableId (right)
		ResultSet<Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> rowsWithRightKeys = engine.joinMixedTypes(dataset, schemaCorrespondences, rowToTableId, keyCorToRightTableId);
		System.out.println(String.format("right key join: %d records", rowsWithRightKeys.size()));
		
		
		// create the join keys
		final Function<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> leftJoinCondition = new Function<List<Object>, Pair<MatchableTableRow,Correspondence<MatchableTableKey,MatchableTableColumn>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public List<Object> execute(
					Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>> record) {
				MatchableTableRow row = record.getFirst();
				MatchableTableKey key = record.getSecond().getFirstRecord();
				
				List<Integer> keyIndices = Q.sort(Q.project(key.getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
				//String keyValue = StringUtils.join(row.get(Q.toPrimitiveIntArray(keyIndices)), "/");
				List<Object> keyValue = Arrays.asList(row.get(Q.toPrimitiveIntArray(keyIndices)));
				
//				return keyValue;
				
				List<Object> joinKeyValue = Q.toList(key.getTableId(), keyIndices, keyValue);
				return joinKeyValue;
				
//				Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> p = new Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>(joinKeyValue, new Pair<>(row, record.getSecond()));
//				return p;
			}
		};
		final Function<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> rightJoinCondition = new Function<List<Object>, Pair<MatchableTableRow,Correspondence<MatchableTableKey,MatchableTableColumn>>>() {			
			
			private static final long serialVersionUID = 1L;

			@Override
			public List<Object> execute(
					Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>> record) {
				MatchableTableRow row = record.getFirst();
				MatchableTableKey key = record.getSecond().getSecondRecord();
				
				//TODO if the columns have different order, this will cause a problem...
				List<Integer> keyIndices = Q.sort(Q.project(key.getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
//				String keyValue = StringUtils.join(row.get(Q.toPrimitiveIntArray(keyIndices)), "/");
				List<Object> keyValue = Arrays.asList(row.get(Q.toPrimitiveIntArray(keyIndices)));
				
//				return keyValue;

				MatchableTableKey leftHandKey = record.getSecond().getFirstRecord();
				List<Integer> leftHandKeyIndices = Q.sort(Q.project(leftHandKey.getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
				List<Object> joinKeyValue = Q.toList(leftHandKey.getTableId(), leftHandKeyIndices, keyValue);
				return joinKeyValue;
				
//				Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> p = new Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>(joinKeyValue, new Pair<>(row,record.getSecond()));
//				return p;
			}
		};
		ResultSet<Pair<Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>> joined = engine.join(rowsWithLeftKeys, rowsWithRightKeys, leftJoinCondition, rightJoinCondition);
		
		
		
		RecordMapper<Pair<Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>, Correspondence<MatchableTableRow, MatchableTableKey>> mapToCorrespondence = new RecordMapper<Pair<Pair<MatchableTableRow,Correspondence<MatchableTableKey,MatchableTableColumn>>,Pair<MatchableTableRow,Correspondence<MatchableTableKey,MatchableTableColumn>>>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableKey>> resultCollector) {
				MatchableTableRow row1 = record.getFirst().getFirst();
				Correspondence<MatchableTableKey, MatchableTableColumn> key1 = record.getFirst().getSecond();
				Correspondence<MatchableTableKey, MatchableTableColumn> key2 = record.getSecond().getSecond();
				MatchableTableRow row2 = record.getSecond().getFirst();

				// the values match, now verify that this actually one of the attribute combinations that we wanted to check (by checking that the values were created from matching key correspondences)
//				if(key1.getFirstRecord().getTableId()==key2.getFirstRecord().getTableId() && key1.getSecondRecord().getTableId()==key2.getSecondRecord().getTableId()) {
//				if(key1.equals(key2)) {
				
					if(row1.getTableId()>row2.getTableId()) {
						System.out.println("Wrong Direction!");
					}
					
					if(row1.getTableId()!=key2.getFirstRecord().getTableId() || row2.getTableId()!=key2.getSecondRecord().getTableId()) {
						
						
						List<Object> left = leftJoinCondition.execute(record.getFirst());
						List<Object> right = rightJoinCondition.execute(record.getSecond());
						
						System.out.println("Incorrect Match!");
					}
					
					if(!key1.equals(key2)) {
						System.out.println("Incorrect Key Match!");
					}
					
					if(row1.getTableId()!=row2.getTableId()) {
						Correspondence<MatchableTableKey, MatchableTableRow> cause = new Correspondence<MatchableTableKey, MatchableTableRow>(key2.getFirstRecord(), key2.getSecondRecord(), key2.getSimilarityScore(), null);
						ResultSet<Correspondence<MatchableTableKey, MatchableTableRow>> causes = new ResultSet<>(Q.toList(cause));
						Correspondence<MatchableTableRow, MatchableTableKey> cor = new Correspondence<MatchableTableRow, MatchableTableKey>(row1, row2, 1.0, causes);
						resultCollector.next(cor);
					}
//				}
			}
		};
		return engine.transform(joined, mapToCorrespondence);
	}
	
}
