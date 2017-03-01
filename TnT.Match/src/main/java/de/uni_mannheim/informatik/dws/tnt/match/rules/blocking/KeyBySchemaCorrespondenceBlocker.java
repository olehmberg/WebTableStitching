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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.P;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Key Propagation: creates key correspondences by adding columns (from correspondences) to one of the keys until they are of equal size
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KeyBySchemaCorrespondenceBlocker<T> {
	
	protected boolean onlyFullKeyMatches = false;

	/**
	 * 
	 */
	public KeyBySchemaCorrespondenceBlocker(boolean onlyFullKeyMatches) {
		this.onlyFullKeyMatches = onlyFullKeyMatches;
	}
	
	public ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> runBlocking(
			BasicCollection<MatchableTableKey> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableColumn, T>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		// first, group the schema correspondences by table combination
		RecordKeyValueMapper<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>, Correspondence<MatchableTableColumn, T>> groupCorrespondences = new RecordKeyValueMapper<Pair<Integer,Integer>, Correspondence<MatchableTableColumn,T>, Correspondence<MatchableTableColumn,T>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableColumn, T> record,
					DatasetIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>> resultCollector) {
			
				resultCollector.next(
						new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableColumn,T>>(
								new Pair<Integer, Integer>(
										record.getFirstRecord().getTableId(), 
										record.getSecondRecord().getTableId()), 
								record));
				
			}
		}; 
		ResultSet<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>> groupedCorrespondences = engine.groupRecords(schemaCorrespondences, groupCorrespondences);
		
		// then, join the keys with the schema correspondences via table id
		// result: key, schema correspondences
		Function<Integer, MatchableTableKey> keyToTableId = new Function<Integer, MatchableTableKey>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableKey input) {
				return input.getTableId();
			}
		};
		Function<Integer, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>> groupToFirstTable = new Function<Integer, Group<Pair<Integer,Integer>,Correspondence<MatchableTableColumn,T>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(
					Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>> input) {
				return input.getKey().getFirst();
			}
		};
		ResultSet<Pair<MatchableTableKey, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>>> join1 = engine.joinMixedTypes(dataset, groupedCorrespondences, keyToTableId, groupToFirstTable);
		
		// the join the result with the keys again via the table id
		// result: key1, schema correspondences, key2
		Function<Integer, Pair<MatchableTableKey, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>>> joinToSecondTable = new Function<Integer, Pair<MatchableTableKey,Group<Pair<Integer,Integer>,Correspondence<MatchableTableColumn,T>>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(
					Pair<MatchableTableKey, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>> input) {
				return input.getSecond().getKey().getSecond();
			}
		};
		ResultSet<Pair<MatchableTableKey, Pair<MatchableTableKey, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>>>> join2 = engine.joinMixedTypes(dataset, join1, keyToTableId, joinToSecondTable);
		
		// validate the keys (check that there are schema correspondences between them)
		// and group the joined data by table combination
		// result table1, table2 -> key1, key2, schema correspondences
		RecordKeyValueMapper<Pair<Integer, Integer>, Pair<MatchableTableKey, Pair<MatchableTableKey, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>>>, Correspondence<MatchableTableKey, MatchableTableColumn>> groupByTablePair = new RecordKeyValueMapper<Pair<Integer,Integer>, Pair<MatchableTableKey,Pair<MatchableTableKey,Group<Pair<Integer,Integer>,Correspondence<MatchableTableColumn,T>>>>, Correspondence<MatchableTableKey,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<MatchableTableKey, Pair<MatchableTableKey, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>>> record,
					DatasetIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableKey, MatchableTableColumn>>> resultCollector) {
				
				ResultSet<Correspondence<MatchableTableColumn, MatchableTableKey>> causes = new ResultSet<>();
				Map<MatchableTableColumn, MatchableTableColumn> mapping = new HashMap<>();
				Map<MatchableTableColumn, MatchableTableColumn> mappingInverse = new HashMap<>();
				
				for(Correspondence<MatchableTableColumn, T> cor : record.getSecond().getSecond().getRecords().get()) {
					causes.add(new Correspondence<MatchableTableColumn, MatchableTableKey>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), null));
					mapping.put(cor.getFirstRecord(), cor.getSecondRecord());
					mappingInverse.put(cor.getSecondRecord(), cor.getFirstRecord());
				}
				
				// get all the columns that have schema correspondences
				Set<MatchableTableColumn> leftColumns = new HashSet<>();
				Set<MatchableTableColumn> rightColumns = new HashSet<>();
				for(Correspondence<MatchableTableColumn, MatchableTableKey> cor : causes.get()) {
					leftColumns.add(cor.getFirstRecord());
					rightColumns.add(cor.getSecondRecord());
				}
				
				MatchableTableKey leftKey = record.getFirst();
				MatchableTableKey rightKey = record.getSecond().getFirst();
				if(leftKey.getTableId()>rightKey.getTableId()) {
					MatchableTableKey tmp = leftKey;
					leftKey = rightKey;
					rightKey = tmp;
				}

				// check if this key combination is supported by the schema correspondences
				// we create a key correspondence if both the left and the right key can be completely mapped
				Set<MatchableTableColumn> leftCorrespondingColumns = new HashSet<>();
				for(MatchableTableColumn col1 : leftKey.getColumns()) {
					MatchableTableColumn col2 = mapping.get(col1);
					if(col2!=null) {
						leftCorrespondingColumns.add(col2);
					} else {
						break;
					}
				}
				if(leftCorrespondingColumns.size()==leftKey.getColumns().size() && !onlyFullKeyMatches) {
					resultCollector.next(
							new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableKey,MatchableTableColumn>>(
									new Pair<Integer, Integer>(leftKey.getTableId(), rightKey.getTableId()), 
									new Correspondence<MatchableTableKey, MatchableTableColumn>(
											leftKey, 
											new MatchableTableKey(rightKey.getTableId(), leftCorrespondingColumns), 
											1.0, 
											causes)));
				}
				
				Set<MatchableTableColumn> rightCorrespondingColumns = new HashSet<>();
				for(MatchableTableColumn col1 : rightKey.getColumns()) {
					MatchableTableColumn col2 = mappingInverse.get(col1);
					if(col2!=null) {
						rightCorrespondingColumns.add(col2);
					} else {
						break;
					}
				}
				if(rightCorrespondingColumns.size()==rightKey.getColumns().size() && !onlyFullKeyMatches) {
					resultCollector.next(
							new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableKey,MatchableTableColumn>>(
									new Pair<Integer, Integer>(leftKey.getTableId(), rightKey.getTableId()),
									new Correspondence<MatchableTableKey, MatchableTableColumn>(
											new MatchableTableKey(leftKey.getTableId(), rightCorrespondingColumns),
											rightKey,
											1.0, 
											causes)));
				}
				
				if(onlyFullKeyMatches) {
					// only propagate if it is still a valid key in both tables
					// the correspondences of the left key must fully include the right key
					boolean rightMatch = Q.intersection(rightKey.getColumns(), leftCorrespondingColumns).size()==rightKey.getColumns().size();
					// the correspondences of the right key must fully include the left key
					boolean leftMatch = Q.intersection(leftKey.getColumns(),  rightCorrespondingColumns).size()==leftKey.getColumns().size();
					
					if(leftMatch && rightMatch) {
						
						resultCollector.next(
								new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableKey,MatchableTableColumn>>(
										record.getSecond().getSecond().getKey(), 
										new Correspondence<MatchableTableKey, MatchableTableColumn>(
												new MatchableTableKey(leftKey.getTableId(), rightCorrespondingColumns),
												new MatchableTableKey(rightKey.getTableId(), leftCorrespondingColumns),
												1.0, 
												causes)));
					}
				}
			}
		};
		ResultSet<Group<Pair<Integer, Integer>, Correspondence<MatchableTableKey, MatchableTableColumn>>> grouped = engine.groupRecords(join2, groupByTablePair);
		
		RecordMapper<Group<Pair<Integer, Integer>, Correspondence<MatchableTableKey, MatchableTableColumn>>, Correspondence<MatchableTableKey, MatchableTableColumn>> resultTransformation = new RecordMapper<Group<Pair<Integer,Integer>,Correspondence<MatchableTableKey,MatchableTableColumn>>, Correspondence<MatchableTableKey,MatchableTableColumn>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Group<Pair<Integer, Integer>, Correspondence<MatchableTableKey, MatchableTableColumn>> record,
					DatasetIterator<Correspondence<MatchableTableKey, MatchableTableColumn>> resultCollector) {
				
				record.getRecords().deduplicate();
				
				
//				// we have all the matching maximal subsets of the keys
//				// now we have to make sure that none of those are subsets of each other (the subset of one key can be a subset of another key, too)
//				
//				Map<Set<MatchableTableColumn>, Correspondence<MatchableTableKey, MatchableTableColumn>> maximalDeterminants = new HashMap<>();
//				for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : record.getRecords().get()) {
//					maximalDeterminants.put(cor.getFirstRecord().getColumns(), cor);
//				}
//				Set<Set<MatchableTableColumn>> keysToCheck = new HashSet<>(maximalDeterminants.keySet());
//				for(Set<MatchableTableColumn> k : maximalDeterminants.keySet()) {
//					keysToCheck.remove(k);
//					
//					if(!Q.any(keysToCheck, new P.ContainsAll<MatchableTableColumn>(k))) {
//						// there is no key in keys that contains all elements of k, so k is minimal
//						resultCollector.next(maximalDeterminants.get(k));
//						
//						// k is a larger key, so we keep it to check the remaining keys
//						keysToCheck.add(k);
//					}
//				}
				
				
				for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : record.getRecords().get()) {
					resultCollector.next(cor);
				}
			}
		};
		
		return engine.transform(grouped, resultTransformation);
	}
	
}
