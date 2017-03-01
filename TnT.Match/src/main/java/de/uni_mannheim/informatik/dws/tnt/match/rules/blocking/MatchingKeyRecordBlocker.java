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
public class MatchingKeyRecordBlocker {

	public ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> runBlocking(
			DataSet<MatchableTableRow, MatchableTableColumn> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> schemaCorrespondences,
			DataProcessingEngine engine) {
	
		//TODO this blocker seems to be too expensive, change computation to aggregation instead of join (group by key values and directly aggregate?)
		
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
		
		ResultSet<Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> rowsWithLeftKeys = engine.joinMixedTypes(dataset, schemaCorrespondences, rowToTableId, keyCorToLeftTableId);
		
		// generate the join key value
		RecordMapper<Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>, Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>> joinKeyMapper = new RecordMapper<Pair<MatchableTableRow,Correspondence<MatchableTableKey, MatchableTableColumn>>, Pair<List<Object>,Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>> record,
					DatasetIterator<Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>> resultCollector) {
				
				MatchableTableRow row = record.getFirst();
				MatchableTableKey key = record.getSecond().getFirstRecord();
				
				List<Integer> keyIndices = Q.sort(Q.project(key.getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
				String keyValue = StringUtils.join(row.get(Q.toPrimitiveIntArray(keyIndices)), "/");
				
				List<Object> joinKeyValue = Q.toList(key.getTableId(), keyIndices, keyValue);
				
				Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> p = new Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>(joinKeyValue, new Pair<>(row, record.getSecond()));
				resultCollector.next(p);
				
			}
		};
		
		ResultSet<Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>> joinLeft = engine.transform(rowsWithLeftKeys, joinKeyMapper);
		
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
		ResultSet<Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> rowsWithRightKeys = engine.joinMixedTypes(dataset, schemaCorrespondences, rowToTableId, keyCorToRightTableId);
		
		// generate the join key value
		RecordMapper<Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>, Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>> joinKey2Mapper = new RecordMapper<Pair<MatchableTableRow,Correspondence<MatchableTableKey, MatchableTableColumn>>, Pair<List<Object>,Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>> record,
					DatasetIterator<Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>> resultCollector) {
				
				MatchableTableRow row = record.getFirst();
				MatchableTableKey key = record.getSecond().getSecondRecord();
				
				//TODO if the columns have different order, this will cause a problem...
				List<Integer> keyIndices = Q.sort(Q.project(key.getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
				String keyValue = StringUtils.join(row.get(Q.toPrimitiveIntArray(keyIndices)), "/");
				
				MatchableTableKey leftHandKey = record.getSecond().getFirstRecord();
				List<Integer> leftHandKeyIndices = Q.sort(Q.project(leftHandKey.getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
				List<Object> joinKeyValue = Q.toList(leftHandKey.getTableId(), leftHandKeyIndices, keyValue);
				
				Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> p = new Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>(joinKeyValue, new Pair<>(row,record.getSecond()));
				resultCollector.next(p);
				
			}
		};
		
		ResultSet<Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>> joinRight = engine.transform(rowsWithRightKeys, joinKey2Mapper);
		
		// then join all rows based on the generated keys
		Function<List<Object>, Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>> getJoinKeyValue = new Function<List<Object>, Pair<List<Object>,Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public List<Object> execute(Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>> input) {
				return input.getFirst();
			}
		};
		ResultSet<Pair<Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>, Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>>> joined = engine.join(joinLeft, joinRight, getJoinKeyValue);
		
		// transform the result to correspondences
		RecordMapper<Pair<Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>, Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>>, Correspondence<MatchableTableRow, MatchableTableKey>> resultMapper = new RecordMapper<Pair<Pair<List<Object>,Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>,Pair<List<Object>,Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>>, Correspondence<MatchableTableRow, MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>, Pair<List<Object>, Pair<MatchableTableRow, Correspondence<MatchableTableKey, MatchableTableColumn>>>> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableKey>> resultCollector) {
				
				MatchableTableRow row1 = record.getFirst().getSecond().getFirst();
				Correspondence<MatchableTableKey, MatchableTableColumn> key1 = record.getFirst().getSecond().getSecond();
				MatchableTableRow row2 = record.getSecond().getSecond().getFirst();
//				Correspondence<MatchableTableKey, MatchableTableColumn> key2 = record.getSecond().getSecond().getSecond();
				
//				ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCors = new ResultSet<>();
//				for(Correspondence<MatchableTableColumn, MatchableTableKey> cause : key1.getCausalCorrespondences().get()) {
//					Correspondence<MatchableTableColumn, MatchableTableRow> schemaCor = new Correspondence<MatchableTableColumn, MatchableTableRow>(cause.getFirstRecord(), cause.getSecondRecord(), cause.getSimilarityScore(), null);
//					schemaCors.add(schemaCor);
//				}
				
				Correspondence<MatchableTableKey, MatchableTableRow> cause = new Correspondence<MatchableTableKey, MatchableTableRow>(key1.getFirstRecord(), key1.getSecondRecord(), key1.getSimilarityScore(), null);
				ResultSet<Correspondence<MatchableTableKey, MatchableTableRow>> causes = new ResultSet<>(Q.toList(cause));
				Correspondence<MatchableTableRow, MatchableTableKey> cor = new Correspondence<MatchableTableRow, MatchableTableKey>(row1, row2, 1.0, causes);
				resultCollector.next(cor);
			}
		};
		
		return engine.transform(joined, resultMapper);
	}
	
}
