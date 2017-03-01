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
import java.util.Set;

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
import de.uni_mannheim.informatik.wdi.processing.ResultSetCollector;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchingKeyRecordBlockerImproved2 {

	public ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> runBlocking(
			DataSet<MatchableTableRow, MatchableTableColumn> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> schemaCorrespondences,
			DataProcessingEngine engine) {
	
		System.out.println("MatchingKeyRecordBlocker");
		System.out.println(String.format("%d key correspondences", schemaCorrespondences.size()));
		
		// create one dataset from the left and right side of the key correspondences ( = all matching keys )
		RecordMapper<Correspondence<MatchableTableKey, MatchableTableColumn>, MatchableTableKey> collectKeys = new RecordMapper<Correspondence<MatchableTableKey,MatchableTableColumn>, MatchableTableKey>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableKey, MatchableTableColumn> record,
					DatasetIterator<MatchableTableKey> resultCollector) {
				resultCollector.next(record.getFirstRecord());
				resultCollector.next(record.getSecondRecord());
			}
		};
		ResultSet<MatchableTableKey> keys = engine.transform(schemaCorrespondences, collectKeys);
		
		System.out.println(String.format("%d join keys", keys.size()));
		keys.deduplicate();
		System.out.println(String.format("%d join keys (distinct)", keys.size()));
		
		// join the records with the keys
		Function<Integer, MatchableTableRow> recordToTableId = new Function<Integer, MatchableTableRow>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableRow input) {
				return input.getTableId();
			}
		};
		Function<Integer, MatchableTableKey> keyToTableId = new Function<Integer, MatchableTableKey>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableKey input) {
				return input.getTableId();
			}
		};
		ResultSet<Pair<MatchableTableRow, MatchableTableKey>> recordsWithKeys = engine.joinMixedTypes(dataset, keys, recordToTableId, keyToTableId);
		System.out.println(String.format("%d record/key combinations", recordsWithKeys.size()));	
		
		// run the blocking: join records via their key values
		Function<List<Object>, Pair<MatchableTableRow, MatchableTableKey>> joinByKeyValue = new Function<List<Object>, Pair<MatchableTableRow,MatchableTableKey>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public List<Object> execute(Pair<MatchableTableRow, MatchableTableKey> input) {
				MatchableTableRow row = input.getFirst();
				MatchableTableKey key = input.getSecond();
				List<Integer> keyIndices = Q.sort(Q.project(key.getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
				List<Object> keyValue = Arrays.asList(row.get(Q.toPrimitiveIntArray(keyIndices)));
				
				if(keyValue==null) {
					System.out.println("null key");
				}
				
				return keyValue;
			}
		};
		ResultSetCollector<Pair<Pair<MatchableTableRow, MatchableTableKey>, Pair<MatchableTableRow, MatchableTableKey>>> joinCollector = new ResultSetCollector<Pair<Pair<MatchableTableRow, MatchableTableKey>, Pair<MatchableTableRow, MatchableTableKey>>>() {

			private static final long serialVersionUID = 1L; 
		
			/* (non-Javadoc)
			 * @see de.uni_mannheim.informatik.wdi.processing.ResultSetCollector#next(java.lang.Object)
			 */
			@Override
			public void next(
					Pair<Pair<MatchableTableRow, MatchableTableKey>, Pair<MatchableTableRow, MatchableTableKey>> record) {
				
				// only keep a joined pair if they're not from the same table
				if(record.getFirst().getFirst().getTableId()!=record.getSecond().getFirst().getTableId()) {
					super.next(record);
				}
			}
		
		};
		ResultSet<Pair<Pair<MatchableTableRow, MatchableTableKey>, Pair<MatchableTableRow, MatchableTableKey>>> blocked = engine.symmetricSelfJoin(recordsWithKeys, joinByKeyValue, joinCollector);
		System.out.println(String.format("record/key join resulted in %d combinations", blocked.size()));
		
		// join with the original key correspondences to filter out additional matches (the values of two keys match by coincidence, but we don't have a key correspondence for that)
		Function<Set<MatchableTableKey>, Pair<Pair<MatchableTableRow, MatchableTableKey>, Pair<MatchableTableRow, MatchableTableKey>>> blockedToKeys = new Function<Set<MatchableTableKey>, Pair<Pair<MatchableTableRow,MatchableTableKey>,Pair<MatchableTableRow,MatchableTableKey>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Set<MatchableTableKey> execute(
					Pair<Pair<MatchableTableRow, MatchableTableKey>, Pair<MatchableTableRow, MatchableTableKey>> input) {
				return Q.toSet(input.getFirst().getSecond(), input.getSecond().getSecond());
			}
		};
		Function<Set<MatchableTableKey>, Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondenceToKeys = new Function<Set<MatchableTableKey>, Correspondence<MatchableTableKey,MatchableTableColumn>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Set<MatchableTableKey> execute(Correspondence<MatchableTableKey, MatchableTableColumn> input) {
				return Q.toSet(input.getFirstRecord(), input.getSecondRecord());
			}
		};
		ResultSet<Pair<Pair<Pair<MatchableTableRow, MatchableTableKey>, Pair<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableKey, MatchableTableColumn>>> blockedWithCor = engine.joinMixedTypes(blocked, schemaCorrespondences, blockedToKeys, keyCorrespondenceToKeys);
		
		// filter out unwanted matches and create correspondences
		RecordMapper<Pair<Pair<Pair<MatchableTableRow, MatchableTableKey>, Pair<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableKey, MatchableTableColumn>>, Correspondence<MatchableTableRow, MatchableTableKey>> filter = new RecordMapper<Pair<Pair<Pair<MatchableTableRow,MatchableTableKey>,Pair<MatchableTableRow,MatchableTableKey>>,Correspondence<MatchableTableKey,MatchableTableColumn>>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Pair<Pair<MatchableTableRow, MatchableTableKey>, Pair<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableKey, MatchableTableColumn>> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableKey>> resultCollector) {

				Correspondence<MatchableTableKey, MatchableTableColumn> keyCor = record.getSecond();
				MatchableTableRow row1 = record.getFirst().getFirst().getFirst();
				MatchableTableKey key1 = record.getFirst().getFirst().getSecond();
				MatchableTableRow row2 = record.getFirst().getSecond().getFirst();
				MatchableTableKey key2 = record.getFirst().getSecond().getSecond();
				
				// filter out matches from the same table
				if(row1.getTableId()!=row2.getTableId()) {
					
					if(row1.getTableId()>row2.getTableId()) {
						MatchableTableRow tmp = row2;
						row2 = row1;
						row1 = tmp;
						
						MatchableTableKey tmpK = key2;
						key2 = key1;
						key1 = tmpK;
					}
					
					if(keyCor.getFirstRecord().getTableId()!=row1.getTableId() && keyCor.getFirstRecord().getTableId()!=row2.getTableId()
							|| keyCor.getSecondRecord().getTableId()!=row1.getTableId() && keyCor.getSecondRecord().getTableId()!=row2.getTableId()) {
						System.out.println("Incorrect join result!'");
					}
					
					ResultSet<Correspondence<MatchableTableKey, MatchableTableRow>> causes = new ResultSet<>();
					Correspondence<MatchableTableKey, MatchableTableRow> cause = new Correspondence<MatchableTableKey, MatchableTableRow>(key1, key2, keyCor.getSimilarityScore(), null);
					causes.add(cause);
					
					Correspondence<MatchableTableRow, MatchableTableKey> cor = new Correspondence<MatchableTableRow, MatchableTableKey>(row1, row2, 1.0, causes);
					resultCollector.next(cor);
				}
			}
		};
		ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> filtered = engine.transform(blockedWithCor, filter);
		System.out.println(String.format("%d combinations after filtering", filtered.size()));
	
		return filtered;
	}
	
}
