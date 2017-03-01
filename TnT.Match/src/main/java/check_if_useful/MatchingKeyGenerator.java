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
package check_if_useful;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Pair;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.P;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableMatchingKey;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchingKeyGenerator {

	public Pair<Collection<TableColumn>, Collection<TableColumn>> generateJoinKeyFromCorrespondences(Table t1, Table t2, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping) {
		
		// convert correspondences to column map
		Map<Integer, Integer> columnCorrespondenceMap = new HashMap<>();
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaMapping.get()) {
			columnCorrespondenceMap.put(cor.getFirstRecord().getColumnIndex(), cor.getSecondRecord().getColumnIndex());
		}
		
		// check schema correspondences: at least one candidate key must have correspondences for all its columns
		// otherwise, we cannot trust the entity links that are generated from these correspondences
		// example:
		// t1 { (1,2), (1,3) }
		// t2 { (2,1), (1,3) }
		// t1 and t2 match using schema-free identity resolution, but will not produce any schema correspondences
		Collection<TableColumn> joinKeyT1=null;
		Collection<TableColumn> joinKeyT2=null;
		for(Collection<TableColumn> t1Key : t1.getSchema().getCandidateKeys()) {
			Collection<TableColumn> correspondingColumns = new HashSet<>();
			
			for(TableColumn c : t1Key) {
				if(columnCorrespondenceMap.containsKey(c.getColumnIndex())) {
					correspondingColumns.add(t2.getSchema().get(columnCorrespondenceMap.get(c.getColumnIndex())));
				}
			}
			
			if(t2.getSchema().getCandidateKeys().contains(correspondingColumns)) {
				// we found a valid join key (a candidate key in both tables with correspondences for all contained columns)
				joinKeyT1 = t1Key;
				joinKeyT2 = correspondingColumns;
				break;
			}
		}
		
		if(joinKeyT1!=null) {
			return new Pair<Collection<TableColumn>, Collection<TableColumn>>(joinKeyT1, joinKeyT2);
		} else {
			return null;
		}
	}

	public Set<WebTableMatchingKey> generateAllJoinKeysFromCorrespondences(Table t1, Table t2, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping, double minSimilarity) {
		
		Set<WebTableMatchingKey> result = new HashSet<>();
		
		// keep track of propagated keys (and replace them after we're done)
		Map<Set<TableColumn>, Set<TableColumn>> propagatedKeys1 = new HashMap<>();
		Map<Set<TableColumn>, Set<TableColumn>> propagatedKeys2 = new HashMap<>();
		
		// convert correspondences to column map
		Map<Integer, Integer> columnCorrespondenceMap = new HashMap<>();
		Map<Integer, Integer> columnCorrespondenceMapReverse = new HashMap<>();
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaMapping.get()) {
			if(cor.getSimilarityScore()>=minSimilarity) {
				columnCorrespondenceMap.put(cor.getFirstRecord().getColumnIndex(), cor.getSecondRecord().getColumnIndex());
				columnCorrespondenceMapReverse.put(cor.getSecondRecord().getColumnIndex(), cor.getFirstRecord().getColumnIndex());
			}
		}
		
		// check schema correspondences: at least one candidate key must have correspondences for all its columns
		// otherwise, we cannot trust the entity links that are generated from these correspondences
		// example:
		// t1 { (1,2), (1,3) }
		// t2 { (2,1), (1,3) }
		// t1 and t2 match using schema-free identity resolution, but will not produce any schema correspondences
		Set<TableColumn> joinKeyT1=null;
		Set<TableColumn> joinKeyT2=null;
		for(Set<TableColumn> t1Key : t1.getSchema().getCandidateKeys()) {
			Set<TableColumn> correspondingColumns = new HashSet<>();
			
			for(TableColumn c : t1Key) {
				if(columnCorrespondenceMap.containsKey(c.getColumnIndex())) {
					correspondingColumns.add(t2.getSchema().get(columnCorrespondenceMap.get(c.getColumnIndex())));
				}
			}
			
			// make sure that there is a corresponding column for each column contained in the key
			if(correspondingColumns.size()==t1Key.size()) {			
				joinKeyT1 = t1Key;
				joinKeyT2 = correspondingColumns;
				
				// equivalence join: both keys have the same size
				if(t2.getSchema().getCandidateKeys().contains(correspondingColumns)) {
				

//				if(Q.any(t2.getSchema().getCandidateKeys(), new P.SetEquals<>(correspondingColumns))) {
					// we found a valid join key (a candidate key in both tables with correspondences for all contained columns)
					result.add(new WebTableMatchingKey(joinKeyT1, joinKeyT2));
				} 
				// left join: a key of t2 contains the key of t1
				else if(Q.any(t2.getSchema().getCandidateKeys(), new P.ContainsAll<>(correspondingColumns))) {
					boolean keySizeIncreased = false;
					
					// figure out if it's a 1:n relation or if we underestimated the smaller key
					// iterate over all keys of t2 that contain the key of t1
					for (Collection<TableColumn> largerKeyT2 : Q.where(t2.getSchema().getCandidateKeys(), new P.ContainsAll<>(correspondingColumns))) {
						Collection<TableColumn> largerKeyT1 = new HashSet<>();
						Collection<TableColumn> noKeyMatch = Q.without(largerKeyT2, correspondingColumns);
						
						// if all columns in the key of t2 which are not in the key of t1 have a correspondence, we can propagate the key to t1
						for(TableColumn c : noKeyMatch) {
							if(columnCorrespondenceMapReverse.containsKey(c.getColumnIndex())) {
								largerKeyT1.add(t1.getSchema().get(columnCorrespondenceMapReverse.get(c.getColumnIndex())));
							}
						}
						
						if(largerKeyT1.size()+joinKeyT1.size()==largerKeyT2.size()) {
							result.add(new WebTableMatchingKey(Q.union(largerKeyT1, joinKeyT1), joinKeyT2));
							keySizeIncreased = true;
							
							propagatedKeys1.put(t1Key, Q.union(largerKeyT1, joinKeyT1));
					
							if(largerKeyT1.size()==0) {
								System.out.println("Key propagation error!");
							}
							
							System.out.println(String.format("Key Propagated (1st): [%d]{%s}U{%s}<->[%d]{%s}",
									t1.getTableId(),
									StringUtils.join(Q.project(joinKeyT1, new TableColumn.ColumnHeaderProjection()), ","),
									StringUtils.join(Q.project(largerKeyT1, new TableColumn.ColumnHeaderProjection()), ","),
									t2.getTableId(),
									StringUtils.join(Q.project(largerKeyT2, new TableColumn.ColumnHeaderProjection()), ",")));
						}
					}
					
					if(!keySizeIncreased) {
						// no chance to propagate the larger key
						// assume 1:n relation
						// result.add(new Pair<Collection<TableColumn>, Collection<TableColumn>>(joinKeyT1, joinKeyT2));
					}
					
				}
				// right join: the key of t1 contains a key of t2
				else if(Q.any(t2.getSchema().getCandidateKeys(), new P.AreAllContainedIn<>(correspondingColumns))) {
					// we already know that all columns of t1's key have a correspondence, so we can directly state the larger key of t2
					result.add(new WebTableMatchingKey(joinKeyT1, joinKeyT2));
					
					for(Set<TableColumn> smallerKeyT2 : Q.where(t2.getSchema().getCandidateKeys(), new P.AreAllContainedIn<>(correspondingColumns))) {
						propagatedKeys2.put(smallerKeyT2, joinKeyT2);
						
						System.out.println(String.format("Key Propagated (2nd): {%s}<->{%s}U{%s}", 
								StringUtils.join(Q.project(joinKeyT1, new TableColumn.ColumnHeaderProjection()), ","),
								StringUtils.join(Q.project(smallerKeyT2, new TableColumn.ColumnHeaderProjection()), ","),
								StringUtils.join(Q.project(Q.without(joinKeyT2, smallerKeyT2), new TableColumn.ColumnHeaderProjection()), ",")));
					}
					
//					Collection<TableColumn> smallerKeyT2 = Q.firstOrDefault(Q.where(t2.getSchema().getCandidateKeys(), new P.AreAllContainedIn<>(correspondingColumns)));
				} 
				// or: the key of t1 is no key of t2 at all (not even a partial one)
				else {
					
					//TODO is it safe to add the correspondences of t1's key as key in t2?
					// a) it could just be a non-minimal key, then the right join case (previous if) would have evaluated to true (at least one of the correspondences is a key in t2)
					// b) it could be non-unique, then we have to somehow combine multiple keys, but which keys should be combined?
					// -- can we apply an adjusted closure calculation over both tables?
					// -- start from the correspondences of t1's key and add more corresponding columns until uniqueness is satisfied (likely not enough correspondences)
					// --> should be done when generating partial matching keys
					
				}
			}
		}
		
		for(Set<TableColumn> oldKey : propagatedKeys1.keySet()) {
			Set<TableColumn> newKey = propagatedKeys1.get(oldKey);
			t1.getSchema().getCandidateKeys().remove(oldKey);
			t1.getSchema().getCandidateKeys().add(newKey);
		}
		for(Set<TableColumn> oldKey : propagatedKeys2.keySet()) {
			Set<TableColumn> newKey = propagatedKeys2.get(oldKey);
			t2.getSchema().getCandidateKeys().remove(oldKey);
			t2.getSchema().getCandidateKeys().add(newKey);
		}
		
		if(result.size()>0) {
			return result;
		} else {
			return null;
		}
	}
	
	public Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> generateAllMatchingKeysFromCorrespondences(int table1Id, int table2Id, Map<Integer,MatchableTableColumn> schema1, Map<Integer,MatchableTableColumn> schema2, Collection<Set<MatchableTableColumn>> candidateKeys1, Collection<Set<MatchableTableColumn>> candidateKeys2, ResultSet<Correspondence<MatchableTableColumn, MatchableTableKey>> schemaMapping, double minSimilarity) {
		
		Set<Correspondence<MatchableTableKey, MatchableTableColumn>> result = new HashSet<>();
		
		// keep track of propagated keys (and replace them after we're done)
		Map<Set<MatchableTableColumn>, Set<MatchableTableColumn>> propagatedKeys1 = new HashMap<>();
		Map<Set<MatchableTableColumn>, Set<MatchableTableColumn>> propagatedKeys2 = new HashMap<>();
		
		// convert correspondences to column map
		Map<Integer, Integer> columnCorrespondenceMap = new HashMap<>();
		Map<Integer, Integer> columnCorrespondenceMapReverse = new HashMap<>();
		for(Correspondence<MatchableTableColumn, MatchableTableKey> cor : schemaMapping.get()) {
			if(cor.getSimilarityScore()>=minSimilarity) {
				columnCorrespondenceMap.put(cor.getFirstRecord().getColumnIndex(), cor.getSecondRecord().getColumnIndex());
				columnCorrespondenceMapReverse.put(cor.getSecondRecord().getColumnIndex(), cor.getFirstRecord().getColumnIndex());
			}
		}
		
		// check schema correspondences: at least one candidate key must have correspondences for all its columns
		// otherwise, we cannot trust the entity links that are generated from these correspondences
		// example:
		// t1 { (1,2), (1,3) }
		// t2 { (2,1), (1,3) }
		// t1 and t2 match using schema-free identity resolution, but will not produce any schema correspondences
		Set<MatchableTableColumn> joinKeyT1=null;
		Set<MatchableTableColumn> joinKeyT2=null;
		for(Set<MatchableTableColumn> t1Key : candidateKeys1) {
			Set<MatchableTableColumn> correspondingColumns = new HashSet<>();
			
			for(MatchableTableColumn c : t1Key) {
				if(columnCorrespondenceMap.containsKey(c.getColumnIndex())) {
					correspondingColumns.add(schema2.get(columnCorrespondenceMap.get(c.getColumnIndex())));
				}
			}
			
			// make sure that there is a corresponding column for each column contained in the key
			if(correspondingColumns.size()==t1Key.size()) {			
				joinKeyT1 = t1Key;
				joinKeyT2 = correspondingColumns;
				
				// equivalence join: both keys have the same size
				if(candidateKeys2.contains(correspondingColumns)) {
				

//				if(Q.any(t2.getSchema().getCandidateKeys(), new P.SetEquals<>(correspondingColumns))) {
					// we found a valid join key (a candidate key in both tables with correspondences for all contained columns)
					result.add(new Correspondence<MatchableTableKey, MatchableTableColumn>(new MatchableTableKey(table1Id, joinKeyT1), new MatchableTableKey(table2Id, joinKeyT2), 1.0, schemaMapping));
				} 
				// left join: a key of t2 contains the key of t1
				else if(Q.any(candidateKeys2, new P.ContainsAll<>(correspondingColumns))) {
					boolean keySizeIncreased = false;
					
					// figure out if it's a 1:n relation or if we underestimated the smaller key
					// iterate over all keys of t2 that contain the key of t1
					for (Collection<MatchableTableColumn> largerKeyT2 : Q.where(candidateKeys2, new P.ContainsAll<>(correspondingColumns))) {
						Collection<MatchableTableColumn> largerKeyT1 = new HashSet<>();
						Collection<MatchableTableColumn> noKeyMatch = Q.without(largerKeyT2, correspondingColumns);
						
						// if all columns in the key of t2 which are not in the key of t1 have a correspondence, we can propagate the key to t1
						for(MatchableTableColumn c : noKeyMatch) {
							if(columnCorrespondenceMapReverse.containsKey(c.getColumnIndex())) {
								largerKeyT1.add(schema1.get(columnCorrespondenceMapReverse.get(c.getColumnIndex())));
							}
						}
						
						if(largerKeyT1.size()+joinKeyT1.size()==largerKeyT2.size()) {
							Set<MatchableTableColumn> propagatedT1 = Q.union(largerKeyT1, joinKeyT1); 
							result.add(new Correspondence<MatchableTableKey, MatchableTableColumn>(new MatchableTableKey(table1Id, propagatedT1), new MatchableTableKey(table2Id, joinKeyT2), 1.0, schemaMapping));
							keySizeIncreased = true;
							
							propagatedKeys1.put(t1Key, Q.union(largerKeyT1, joinKeyT1));
					
							if(largerKeyT1.size()==0) {
								System.out.println("Key propagation error!");
							}
							
							System.out.println(String.format("Key Propagated (1st): [%d]{%s}U{%s}<->[%d]{%s}",
									table1Id,
									StringUtils.join(Q.project(joinKeyT1, new MatchableTableColumn.ColumnHeaderProjection()), ","),
									StringUtils.join(Q.project(largerKeyT1, new MatchableTableColumn.ColumnHeaderProjection()), ","),
									table2Id,
									StringUtils.join(Q.project(largerKeyT2, new MatchableTableColumn.ColumnHeaderProjection()), ",")));
						}
					}
					
					if(!keySizeIncreased) {
						// no chance to propagate the larger key
						// assume 1:n relation
						// result.add(new Pair<Collection<TableColumn>, Collection<TableColumn>>(joinKeyT1, joinKeyT2));
					}
					
				}
				// right join: the key of t1 contains a key of t2
				else if(Q.any(candidateKeys2, new P.AreAllContainedIn<>(correspondingColumns))) {
					// we already know that all columns of t1's key have a correspondence, so we can directly state the larger key of t2
					result.add(new Correspondence<MatchableTableKey, MatchableTableColumn>(new MatchableTableKey(table1Id, joinKeyT1), new MatchableTableKey(table2Id, joinKeyT2), 1.0, schemaMapping));
					
					for(Set<MatchableTableColumn> smallerKeyT2 : Q.where(candidateKeys2, new P.AreAllContainedIn<>(correspondingColumns))) {
						propagatedKeys2.put(smallerKeyT2, joinKeyT2);
						
						System.out.println(String.format("Key Propagated (2nd): {%s}<->{%s}U{%s}", 
								StringUtils.join(Q.project(joinKeyT1, new MatchableTableColumn.ColumnHeaderProjection()), ","),
								StringUtils.join(Q.project(smallerKeyT2, new MatchableTableColumn.ColumnHeaderProjection()), ","),
								StringUtils.join(Q.project(Q.without(joinKeyT2, smallerKeyT2), new MatchableTableColumn.ColumnHeaderProjection()), ",")));
					}
					
//					Collection<TableColumn> smallerKeyT2 = Q.firstOrDefault(Q.where(t2.getSchema().getCandidateKeys(), new P.AreAllContainedIn<>(correspondingColumns)));
				} 
				// or: the key of t1 is no key of t2 at all (not even a partial one)
				else {
					
					//TODO is it safe to add the correspondences of t1's key as key in t2?
					// a) it could just be a non-minimal key, then the right join case (previous if) would have evaluated to true (at least one of the correspondences is a key in t2)
					// b) it could be non-unique, then we have to somehow combine multiple keys, but which keys should be combined?
					// -- can we apply an adjusted closure calculation over both tables?
					// -- start from the correspondences of t1's key and add more corresponding columns until uniqueness is satisfied (likely not enough correspondences)
					// --> should be done when generating partial matching keys
					
				}
			}
		}

		if(result.size()>0) {
			return result;
		} else {
			return null;
		}
	}
	
	public void removeInvalidCandidateKeys(Table t1, Table t2, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping, double minSimilarity) {
		
		// keep track of propagated keys (and replace them after we're done)
		Set<Set<TableColumn>> invalidatedKeys = new HashSet<>();
		
		// convert correspondences to column map
		Map<Integer, Integer> columnCorrespondenceMap = new HashMap<>();
		Map<Integer, Integer> columnCorrespondenceMapReverse = new HashMap<>();
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaMapping.get()) {
			if(cor.getSimilarityScore()>=minSimilarity) {
				columnCorrespondenceMap.put(cor.getFirstRecord().getColumnIndex(), cor.getSecondRecord().getColumnIndex());
				columnCorrespondenceMapReverse.put(cor.getSecondRecord().getColumnIndex(), cor.getFirstRecord().getColumnIndex());
			}
		}
		
		// check schema correspondences: at least one candidate key must have correspondences for all its columns
		// otherwise, we cannot trust the entity links that are generated from these correspondences
		// example:
		// t1 { (1,2), (1,3) }
		// t2 { (2,1), (1,3) }
		// t1 and t2 match using schema-free identity resolution, but will not produce any schema correspondences
		for(Set<TableColumn> t1Key : t1.getSchema().getCandidateKeys()) {
			Set<TableColumn> correspondingColumns = new HashSet<>();
			
			for(TableColumn c : t1Key) {
				if(columnCorrespondenceMap.containsKey(c.getColumnIndex())) {
					correspondingColumns.add(t2.getSchema().get(columnCorrespondenceMap.get(c.getColumnIndex())));
				}
			}
			
			// make sure that there is a corresponding column for each column contained in the key
			if(correspondingColumns.size()==t1Key.size()) {			
				
				// equivalence join: both keys have the same size
				if(t2.getSchema().getCandidateKeys().contains(correspondingColumns)) {
				} 
				// left join: a key of t2 contains the key of t1
				else if(Q.any(t2.getSchema().getCandidateKeys(), new P.ContainsAll<>(correspondingColumns))) {
				}
				// right join: the key of t1 contains a key of t2
				else if(Q.any(t2.getSchema().getCandidateKeys(), new P.AreAllContainedIn<>(correspondingColumns))) {
				} 
				// or: the key of t1 is no key of t2 at all (not even a partial one)
				else {
					invalidatedKeys.add(t1Key);
				}
			}
		}
		
		for(Set<TableColumn> oldKey : invalidatedKeys) {
			t1.getSchema().getCandidateKeys().remove(oldKey);
			System.out.println(String.format("Invalidated Key: {%s} %s", t1.getPath(), Q.project(oldKey, new TableColumn.ColumnHeaderProjection())));
		}
	}
	
	public Set<WebTableMatchingKey> generateMaximalSubJoinKeysFromCorrespondences(Table t1, Table t2, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping, double minSimilarity) {
		
		Set<WebTableMatchingKey> result = new HashSet<>();
		
		// alternative calculation: create a matching key from all schema correspondences and keep only those, which appear in at least one candidate key
		final Set<TableColumn> t1Cols = new HashSet<>();
		final Set<TableColumn> t2Cols = new HashSet<>();
		
		Set<Set<TableColumn>> t1Keys = new HashSet<>();
		Set<Set<TableColumn>> t2Keys = new HashSet<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaMapping.get()) {
			TableColumn col1 = t1.getSchema().get(cor.getFirstRecord().getColumnIndex());
			TableColumn col2 = t2.getSchema().get(cor.getSecondRecord().getColumnIndex());
			
			Collection<Set<TableColumn>> t1Matches = Q.where(t1.getSchema().getCandidateKeys(), new P.Contains<>(col1));
			Collection<Set<TableColumn>> t2Matches = Q.where(t2.getSchema().getCandidateKeys(), new P.Contains<>(col2));
			
			if(t1Matches.size()>0 || t2Matches.size()>0) {
				
				t1Cols.add(col1);
				t2Cols.add(col2);
				
				t1Keys.addAll(t1Matches);
				t2Keys.addAll(t2Matches);
			}
		}
		
		if(t1Cols.size()>0) {
			result.add(new WebTableMatchingKey(t1Cols, t2Cols));
		}
		
		// find a key that contains the partial key that was just generated for both tables, add that key and remove contained keys???
		// just merge all columns of all participating keys and reduce them to form a minimal superkey?
		
//		Collection<TableColumn> bestT1KeyMatch = Q.max(t1.getSchema().getCandidateKeys(), new Func<Integer, Set<TableColumn>>() {
//
//			@Override
//			public Integer invoke(Set<TableColumn> in) {
//				return Q.intersection(in, t1Cols).size();
//			}
//		});
//		Collection<TableColumn> bestT2KeyMatch = Q.max(t2.getSchema().getCandidateKeys(), new Func<Integer, Set<TableColumn>>() {
//
//			@Override
//			public Integer invoke(Set<TableColumn> in) {
//				return Q.intersection(in, t1Cols).size();
//			}
//		});
		
		// both keys in the matching key must correspond to an existing key in the tables, which we can maybe extend
		
		
		
//		// convert correspondences to column map
//		Map<Integer, Integer> columnCorrespondenceMap = new HashMap<>();
//		Map<Integer, Integer> columnCorrespondenceMapReverse = new HashMap<>();
//		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaMapping.get()) {
//			if(cor.getSimilarityScore()>=minSimilarity) {
//				columnCorrespondenceMap.put(cor.getFirstRecord().getColumnIndex(), cor.getSecondRecord().getColumnIndex());
//				columnCorrespondenceMapReverse.put(cor.getSecondRecord().getColumnIndex(), cor.getFirstRecord().getColumnIndex());
//			}
//		}
//		
//		// check schema correspondences: at least one candidate key must have correspondences for all its columns
//		// otherwise, we cannot trust the entity links that are generated from these correspondences
//		// example:
//		// t1 { (1,2), (1,3) }
//		// t2 { (2,1), (1,3) }
//		// t1 and t2 match using schema-free identity resolution, but will not produce any schema correspondences
//		Collection<TableColumn> joinKeyT1=null;
//		Collection<TableColumn> joinKeyT2=null;
//		for(Collection<TableColumn> t1Key : t1.getSchema().getCandidateKeys()) {
//			Collection<TableColumn> correspondingColumns = new LinkedList<>();
//			
//			for(TableColumn c : t1Key) {
//				if(columnCorrespondenceMap.containsKey(c.getColumnIndex())) {
//					correspondingColumns.add(t2.getSchema().get(columnCorrespondenceMap.get(c.getColumnIndex())));
//				}
//			}
//			
//			// at least one of the keys is not completely mapped
//			if(correspondingColumns.size()!=t1Key.size() && correspondingColumns.size()>0) {			
//				joinKeyT1 = t1Key;
//				joinKeyT2 = correspondingColumns;
//				
//				// equivalence join: all mapped columns of t1's key form a key in t2 (n:1 relationship from t1 to t2)
//				if(t2.getSchema().getCandidateKeys().contains(correspondingColumns)) {
//					result.add(new Pair<>(joinKeyT1, joinKeyT2));
//				} 
//				// partial join: a key of t2 contains the mapped part of the key of t1
//				else if(Q.any(t2.getSchema().getCandidateKeys(), new P.ContainsAll<>(correspondingColumns))) {
//					
//					// for all unmapped columns of t2's matching keys, check if there is a mapping to t1
//					for (Collection<TableColumn> largerKeyT2 : Q.where(t2.getSchema().getCandidateKeys(), new P.ContainsAll<>(correspondingColumns))) {
//						Collection<TableColumn> largerKeyT1 = new LinkedList<>();
//						Collection<TableColumn> noKeyMatch = Q.without(largerKeyT2, correspondingColumns);
//						
//						// if all columns in the key of t2 which are not in the key of t1 have a correspondence, we can propagate the key to t1
//						for(TableColumn c : noKeyMatch) {
//							if(columnCorrespondenceMapReverse.containsKey(c.getColumnIndex())) {
//								largerKeyT1.add(t1.getSchema().get(columnCorrespondenceMapReverse.get(c.getColumnIndex())));
//							}
//						}
//						
//						result.add(new Pair<Collection<TableColumn>, Collection<TableColumn>>(Q.union(largerKeyT1, joinKeyT1), joinKeyT2));
//					}
//					
//					
////					result.add(new Pair<>(joinKeyT1, joinKeyT2));
//				}
//				// all mapped columns of t1's key contain a complete key of t2 (t2's key is a subset of t1's key's mapped columns) 
//				else if(Q.any(t2.getSchema().getCandidateKeys(), new P.AreAllContainedIn<>(correspondingColumns))) {
//					result.add(new Pair<>(joinKeyT1, joinKeyT2));
//				}
//			}
//		}
		
		if(result.size()>0) {
			return result;
		} else {
			return null;
		}
	}
	
}
