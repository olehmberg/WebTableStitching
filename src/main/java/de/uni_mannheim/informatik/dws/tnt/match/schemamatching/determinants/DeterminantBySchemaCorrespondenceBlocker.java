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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.determinants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * Key Propagation: creates key correspondences by adding columns (from correspondences) to one of the keys until they are of equal size
 * 
 * joins keys via schema correspondences: [key1:a1,b1,c1]-[correspondences:a1-a2,b1-b2,c1-c2]-[key2:a2,b2]
 * and then propagates the key information via the correspodences:[key1:a1,b1,c1]-[key2:a2,b2,*c2*]
 * 
 * alternative(?): join keys with correspondences, then use mapped attributes as blocking keys
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DeterminantBySchemaCorrespondenceBlocker {
	
	protected boolean onlyFullKeyMatches = false;

	/**
	 * 
	 */
	public DeterminantBySchemaCorrespondenceBlocker(boolean onlyFullKeyMatches) {
		this.onlyFullKeyMatches = onlyFullKeyMatches;
	}
	
	public Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> runBlocking(
			DataSet<MatchableTableDeterminant, MatchableTableColumn> dataset,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		// first, group the schema correspondences by table combination
		RecordKeyValueMapper<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>, Correspondence<MatchableTableColumn, Matchable>> groupCorrespondences = new RecordKeyValueMapper<Pair<Integer,Integer>, Correspondence<MatchableTableColumn,Matchable>, Correspondence<MatchableTableColumn,Matchable>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(Correspondence<MatchableTableColumn, Matchable> record,
					DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) {
			
				resultCollector.next(
						new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableColumn,Matchable>>(
								new Pair<Integer, Integer>(
										record.getFirstRecord().getTableId(), 
										record.getSecondRecord().getTableId()), 
								record));
				
			}
		}; 
		Processable<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> groupedCorrespondences = schemaCorrespondences.group(groupCorrespondences);
		
		// then, join the keys with the schema correspondences via table id
		// result: key, schema correspondences
		Function<Integer, MatchableTableDeterminant> keyToTableId = new Function<Integer, MatchableTableDeterminant>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableDeterminant input) {
				return input.getTableId();
			}
		};
		Function<Integer, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> groupToFirstTable = new Function<Integer, Group<Pair<Integer,Integer>,Correspondence<MatchableTableColumn,Matchable>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(
					Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>> input) {
				return input.getKey().getFirst();
			}
		};
		Processable<Pair<MatchableTableDeterminant, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>>> join1 = dataset.join(groupedCorrespondences, keyToTableId, groupToFirstTable);
		
		// the join the result with the keys again via the table id
		// result: key1, schema correspondences, key2
		Function<Integer, Pair<MatchableTableDeterminant, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>>> joinToSecondTable = new Function<Integer, Pair<MatchableTableDeterminant,Group<Pair<Integer,Integer>,Correspondence<MatchableTableColumn,Matchable>>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(
					Pair<MatchableTableDeterminant, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> input) {
				return input.getSecond().getKey().getSecond();
			}
		};
		Processable<Pair<MatchableTableDeterminant, Pair<MatchableTableDeterminant, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>>>> join2 = dataset.join(join1, keyToTableId, joinToSecondTable);
		
		// validate the keys (check that there are schema correspondences between them)
		// and group the joined data by table combination
		// result table1, table2 -> key1, key2, schema correspondences
		RecordKeyValueMapper<Pair<Integer, Integer>, Pair<MatchableTableDeterminant, Pair<MatchableTableDeterminant, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>>>, Correspondence<MatchableTableDeterminant, MatchableTableColumn>> groupByTablePair = new RecordKeyValueMapper<Pair<Integer,Integer>, Pair<MatchableTableDeterminant,Pair<MatchableTableDeterminant,Group<Pair<Integer,Integer>,Correspondence<MatchableTableColumn,Matchable>>>>, Correspondence<MatchableTableDeterminant,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(
					Pair<MatchableTableDeterminant, Pair<MatchableTableDeterminant, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>>> record,
					DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableDeterminant, MatchableTableColumn>>> resultCollector) {
				
				//Pair<MatchableTableKey, Pair<MatchableTableKey, Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, T>>>>
				// is just a complicated way of stating "correspondence between two keys with correspondences between columns as cause"
				
				Processable<Correspondence<MatchableTableColumn, MatchableTableDeterminant>> causes = new ProcessableCollection<>();
				Map<MatchableTableColumn, MatchableTableColumn> mapping = new HashMap<>();
				Map<MatchableTableColumn, MatchableTableColumn> mappingInverse = new HashMap<>();
				
				for(Correspondence<MatchableTableColumn, Matchable> cor : record.getSecond().getSecond().getRecords().get()) {
					causes.add(new Correspondence<MatchableTableColumn, MatchableTableDeterminant>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), null));
					mapping.put(cor.getFirstRecord(), cor.getSecondRecord());
					mappingInverse.put(cor.getSecondRecord(), cor.getFirstRecord());
				}
				
				// get all the columns that have schema correspondences
				Set<MatchableTableColumn> leftColumns = new HashSet<>();
				Set<MatchableTableColumn> rightColumns = new HashSet<>();
				for(Correspondence<MatchableTableColumn, MatchableTableDeterminant> cor : causes.get()) {
					leftColumns.add(cor.getFirstRecord());
					rightColumns.add(cor.getSecondRecord());
				}
				
				MatchableTableDeterminant leftKey = record.getFirst();
				MatchableTableDeterminant rightKey = record.getSecond().getFirst();
				if(leftKey.getTableId()>rightKey.getTableId()) {
					MatchableTableDeterminant tmp = leftKey;
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
							new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableDeterminant,MatchableTableColumn>>(
									new Pair<Integer, Integer>(leftKey.getTableId(), rightKey.getTableId()), 
									new Correspondence<MatchableTableDeterminant, MatchableTableColumn>(
											leftKey, 
											new MatchableTableDeterminant(rightKey.getTableId(), leftCorrespondingColumns), 
											1.0, 
											Correspondence.toMatchable(causes))));
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
							new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableDeterminant,MatchableTableColumn>>(
									new Pair<Integer, Integer>(leftKey.getTableId(), rightKey.getTableId()),
									new Correspondence<MatchableTableDeterminant, MatchableTableColumn>(
											new MatchableTableDeterminant(leftKey.getTableId(), rightCorrespondingColumns),
											rightKey,
											1.0, 
											Correspondence.toMatchable(causes))));
				}
				
				if(onlyFullKeyMatches) {
					// only propagate if it is still a valid key in both tables
					// the correspondences of the left key must fully include the right key
					boolean rightMatch = Q.intersection(rightKey.getColumns(), leftCorrespondingColumns).size()==rightKey.getColumns().size();
					// the correspondences of the right key must fully include the left key
					boolean leftMatch = Q.intersection(leftKey.getColumns(),  rightCorrespondingColumns).size()==leftKey.getColumns().size();
					
					if(leftMatch && rightMatch) {
						
						causes = causes.where(
								(c) -> 
									(rightCorrespondingColumns.contains(c.getFirstRecord()) || leftCorrespondingColumns.contains(c.getFirstRecord()))
									&&
									(rightCorrespondingColumns.contains(c.getSecondRecord()) || leftCorrespondingColumns.contains(c.getSecondRecord()))
								);
						
						resultCollector.next(
								new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableDeterminant,MatchableTableColumn>>(
										record.getSecond().getSecond().getKey(), 
										new Correspondence<MatchableTableDeterminant, MatchableTableColumn>(
												new MatchableTableDeterminant(leftKey.getTableId(), rightCorrespondingColumns),
												new MatchableTableDeterminant(rightKey.getTableId(), leftCorrespondingColumns),
												1.0, 
												Correspondence.toMatchable(causes))));
					}
				}
			}
		};
		Processable<Group<Pair<Integer, Integer>, Correspondence<MatchableTableDeterminant, MatchableTableColumn>>> grouped = join2.group(groupByTablePair);
		
		RecordMapper<Group<Pair<Integer, Integer>, Correspondence<MatchableTableDeterminant, MatchableTableColumn>>, Correspondence<MatchableTableDeterminant, MatchableTableColumn>> resultTransformation = new RecordMapper<Group<Pair<Integer,Integer>,Correspondence<MatchableTableDeterminant,MatchableTableColumn>>, Correspondence<MatchableTableDeterminant,MatchableTableColumn>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Group<Pair<Integer, Integer>, Correspondence<MatchableTableDeterminant, MatchableTableColumn>> record,
					DataIterator<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> resultCollector) {
				
//				record.getRecords().deduplicate();
				
				
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
				
				
				for(Correspondence<MatchableTableDeterminant, MatchableTableColumn> cor : record.getRecords().distinct().get()) {
					resultCollector.next(cor);
				}
			}
		};
		
		return grouped.map(resultTransformation);
	}
	
}
