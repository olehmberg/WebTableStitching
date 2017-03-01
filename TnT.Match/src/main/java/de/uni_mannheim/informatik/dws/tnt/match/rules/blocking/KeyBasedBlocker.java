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

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
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
public class KeyBasedBlocker {

	public static class TableKeyWithValue implements Comparable<TableKeyWithValue> {
		public Set<MatchableTableColumn> key;
		public String value;
		
		/**
		 * 
		 */
		public TableKeyWithValue(Set<MatchableTableColumn> key, String value) {
			this.key = key;
			this.value = value;
		}

		/* (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(TableKeyWithValue o) {
			return value.compareTo(o.value);
		}
	}
	
	public static class TableKeyWithRow {
		public MatchableTableKey key;
		public MatchableTableRow row;
		/**
		 * @param key
		 * @param row
		 */
		public TableKeyWithRow(MatchableTableKey key, MatchableTableRow row) {
			super();
			this.key = key;
			this.row = row;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			result = prime * result + ((row == null) ? 0 : row.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TableKeyWithRow other = (TableKeyWithRow) obj;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			if (row == null) {
				if (other.row != null)
					return false;
			} else if (!row.equals(other.row))
				return false;
			return true;
		}
		
		
	}

	public ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> runBlocking(
			DataSet<MatchableTableRow, MatchableTableColumn> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences,
			DataProcessingEngine engine) {

		// first determine which combinations of attributes will produce record links

		// bottom-up approach is too slow (checks too many unneeded combinations)
		// try top-down approach:
		// - run blocking as usual
		// - only for tables without connection (instance correspondences), try sub-key combinations
		// -- create subsets from the larger keys to match the size of the smaller keys
		// -- create subsets from both tables' key to also match subkeys of the smaller keys
		// procedure:
		// - run recordLinkMapper with full candidate keys
		// - filter out the matching table combinations
		// - on groups (!) of records for table combinations without matches, create subkey matches specifically for that combination
//		
//		RecordKeyValueMapper<String, MatchableTableRow, TableKeyWithRow> recordLinkMapper = new RecordKeyValueMapper<String, MatchableTableRow, TableKeyWithRow>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(MatchableTableRow record,
//					DatasetIterator<Pair<String, TableKeyWithRow>> resultCollector) {
//				
//				// check single attributes
//				
//				Set<Set<MatchableTableColumn>> possibleDeterminants = new HashSet<>();
//				for(MatchableTableColumn[] key : record.getKeys()) {
//					
//					for(MatchableTableColumn col : key) {						
//						possibleDeterminants.add(Q.toSet(col));
//					}
//					
//				}
//				
//				for(Set<MatchableTableColumn> keyCols : possibleDeterminants) {
//					int[] indices = Q.toPrimitiveIntArray(Q.project(keyCols, new MatchableTableColumn.ColumnIndexProjection()));
//					String value = StringUtils.join(Q.sort(Q.toString(Q.toList(record.get(indices)))), "/");
//					
//					if(!value.isEmpty()) {
//						MatchableTableKey key = new MatchableTableKey(record.getTableId(), keyCols);
//						resultCollector.next(new Pair<String, TableKeyWithRow>(value, new TableKeyWithRow(key, record)));
//					}
//				}
//			}
//		};
//		
//		DataAggregator<TableKeyWithRow, Set<TableKeyWithRow>> combinationAggregator = new DataAggregator<TableKeyWithRow, Set<TableKeyWithRow>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Set<TableKeyWithRow> aggregate(Set<TableKeyWithRow> previousResult, TableKeyWithRow record) {
//				if(previousResult==null) {
//					previousResult = new HashSet<>();
//				}
//				
//				previousResult.add(record);
//				
//				return previousResult;
//			}
//		};
//		// counts the frequency of each attribute/value combination
//		ResultSet<Pair<String, Set<TableKeyWithRow>>> counts = engine.aggregateRecords(dataset, recordLinkMapper, combinationAggregator);
//		
//		counts = engine.filter(counts, new Function<Boolean,Pair<String, Set<TableKeyWithRow>>>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Boolean execute(Pair<String, Set<TableKeyWithRow>> input) {
//				return input.getSecond().size()>1;
//			}});
		
		// we have: value -> { (key1, row1), (key2, row2), ... }
		// now re-group by attribute combinations
//		RecordKeyValueMapper<Collection<MatchableTableKey>, Pair<String, Set<TableKeyWithRow>>, Pair<MatchableTableRow,MatchableTableRow>> groupCountsByAttributeCombination = new RecordKeyValueMapper<Collection<MatchableTableKey>, Pair<String,Set<TableKeyWithRow>>, Pair<MatchableTableRow,MatchableTableRow>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Pair<String, Set<TableKeyWithRow>> record,
//					DatasetIterator<Pair<Collection<MatchableTableKey>, Pair<MatchableTableRow,MatchableTableRow>>> resultCollector) {
//				
//				// for each attribute (combination) that matches for a value (an hence is in record)
//				// create a new entry for a row combination
//				
//				List<TableKeyWithRow> matches = new ArrayList<>(record.getSecond());
//				
//				// make sure the smaller table ids are at the beginning of the list, so the order is maintained (and we always know the direction of the links)
//				Q.sort(matches, new Comparator<TableKeyWithRow>() {
//
//					@Override
//					public int compare(TableKeyWithRow o1, TableKeyWithRow o2) {
//						return Integer.compare(o1.row.getTableId(), o2.row.getTableId());
//					}
//				});
//				
//				for(int i=0;i<matches.size();i++) {
//					TableKeyWithRow k1 = matches.get(i);
//					
//					for(int j=i+1;j<matches.size();j++) {
//						TableKeyWithRow k2 = matches.get(j);
//						
//						if(k1.row.getTableId()!=k2.row.getTableId()) {
////							List<Integer> rows = Q.toList(k1.row.getTableId(), k1.row.getRowNumber(), k2.row.getTableId(), k2.row.getRowNumber());
//							List<MatchableTableKey> rows = Q.toList(k1.key, k2.key);
//							Pair<MatchableTableRow,MatchableTableRow> p = new Pair<>(k1.row, k2.row);
//							resultCollector.next(new Pair<Collection<MatchableTableKey>, Pair<MatchableTableRow,MatchableTableRow>>(rows, p));
//						}
//					}
//				}
//				
//			}
//		};
//		DataAggregator<Pair<MatchableTableRow,MatchableTableRow>, Set<Pair<MatchableTableRow,MatchableTableRow>>> combinationAggregatorAttribute = new DataAggregator<Pair<MatchableTableRow,MatchableTableRow>, Set<Pair<MatchableTableRow,MatchableTableRow>>>() {
//			
//			@Override
//			public Set<Pair<MatchableTableRow,MatchableTableRow>> aggregate(
//					Set<Pair<MatchableTableRow,MatchableTableRow>> previousResult,
//					Pair<MatchableTableRow,MatchableTableRow> record) {
//				
//				Set<Pair<MatchableTableRow,MatchableTableRow>> result = previousResult;
//				
//				if(previousResult==null) {
//					result = new HashSet<>();
//				}
//				
//				result.add(record);
//				
//				return result;
//			}
//		};
//		ResultSet<Pair<Collection<MatchableTableKey>, Set<Pair<MatchableTableRow,MatchableTableRow>>>> attributeMatches = engine.aggregateRecords(counts, groupCountsByAttributeCombination, combinationAggregatorAttribute);
//		ResultSet<Pair<Collection<MatchableTableKey>, Integer>> attributeMatches = engine.aggregateRecords(counts, groupCountsByAttributeCombination, new CountAggregator<Pair<MatchableTableRow,MatchableTableRow>>());
		
		// we have: value -> { (key1, row1), (key2, row2), ... }
		// now re-group by row combinations
//		RecordKeyValueMapper<Collection<Integer>, Pair<String, Set<TableKeyWithRow>>, Pair<MatchableTableKey,MatchableTableKey>> groupCountsByRowCombination = new RecordKeyValueMapper<Collection<Integer>, Pair<String,Set<TableKeyWithRow>>, Pair<MatchableTableKey,MatchableTableKey>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Pair<String, Set<TableKeyWithRow>> record,
//					DatasetIterator<Pair<Collection<Integer>, Pair<MatchableTableKey,MatchableTableKey>>> resultCollector) {
//				
//				// for each attribute (combination) that matches for a value (an hence is in record)
//				// create a new entry for a row combination
//				
//				List<TableKeyWithRow> matches = new ArrayList<>(record.getSecond());
//				
//				// make sure the smaller table ids are at the beginning of the list, so the order is maintained (and we always know the direction of the links)
//				Q.sort(matches, new Comparator<TableKeyWithRow>() {
//
//					@Override
//					public int compare(TableKeyWithRow o1, TableKeyWithRow o2) {
//						return Integer.compare(o1.row.getTableId(), o2.row.getTableId());
//					}
//				});
//				
//				for(int i=0;i<matches.size();i++) {
//					TableKeyWithRow k1 = matches.get(i);
//					
//					for(int j=i+1;j<matches.size();j++) {
//						TableKeyWithRow k2 = matches.get(j);
//						
//						if(k1.row.getTableId()!=k2.row.getTableId()) {
////							List<Integer> rows = Q.toList(k1.row.getTableId(), k1.row.getRowNumber(), k2.row.getTableId(), k2.row.getRowNumber());
//							List<Integer> rows = Q.toList(k1.row.getTableId(), k2.row.getTableId());
//							Pair<MatchableTableKey,MatchableTableKey> p = new Pair<>(k1.key, k2.key);
//							resultCollector.next(new Pair<Collection<Integer>, Pair<MatchableTableKey,MatchableTableKey>>(rows, p));
//						}
//					}
//				}
//				
//			}
//		};
//		DataAggregator<Pair<MatchableTableKey, MatchableTableKey>, Pair<MatchableTableKey, MatchableTableKey>> combinationAggregator2 = new DataAggregator<Pair<MatchableTableKey,MatchableTableKey>, Pair<MatchableTableKey,MatchableTableKey>>() {
//			
//			@Override
//			public Pair<MatchableTableKey, MatchableTableKey> aggregate(
//					Pair<MatchableTableKey, MatchableTableKey> previousResult,
//					Pair<MatchableTableKey, MatchableTableKey> record) {
//				
//				if(previousResult==null) {
//					MatchableTableKey k1 = new MatchableTableKey(record.getFirst().getTableId(), new HashSet<>(record.getFirst().getColumns()));
//					MatchableTableKey k2 = new MatchableTableKey(record.getSecond().getTableId(), new HashSet<>(record.getSecond().getColumns()));
//					
//					return new Pair<MatchableTableKey, MatchableTableKey>(k1, k2);	
//				} else {
//					previousResult.getFirst().getColumns().addAll(record.getFirst().getColumns());
//					previousResult.getSecond().getColumns().addAll(record.getSecond().getColumns());
//					
//					return previousResult;
//				}
//			}
//		};
//		ResultSet<Pair<Collection<Integer>, Pair<MatchableTableKey, MatchableTableKey>>> keyMatches = engine.aggregateRecords(counts, groupCountsByRowCombination, combinationAggregator2);
		
		
		RecordMapper<MatchableTableRow, Pair<TableKeyWithValue, MatchableTableRow>> blockingMapper = new RecordMapper<MatchableTableRow, Pair<TableKeyWithValue,MatchableTableRow>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(MatchableTableRow record,
					DatasetIterator<Pair<TableKeyWithValue, MatchableTableRow>> resultCollector) {
				
				// this variant creates links if any two determinants match
//				// create a list of all candidate keys and their subsets
//				Set<Set<MatchableTableColumn>> possibleDeterminants = new HashSet<>();
//				for(MatchableTableColumn[] key : record.getKeys()) {
//					possibleDeterminants.add(Q.toSet(key));
//					
//					for(MatchableTableColumn col : key) {
//						Set<MatchableTableColumn> subset = Q.toSet(key);
//						subset.remove(col);
//						possibleDeterminants.add(subset);
//					}
//					possibleDeterminants.addAll(Q.getAllSubsets(Q.toSet(key)));
//				}
//				
//				Iterator<Set<MatchableTableColumn>> detIt = possibleDeterminants.iterator();
//				while(detIt.hasNext()) {
//					// exclude subsets of size 1
//					if(detIt.next().size()<=1) {
//						detIt.remove();
//					}
//				}
//				
//				// except if they are the full candidate key
////				for(MatchableTableColumn[] key : record.getKeys()) {
////					possibleDeterminants.add(Q.toSet(key));
////				}
//				
//				for(Set<MatchableTableColumn> keyCols : possibleDeterminants) {
//					int[] indices = Q.toPrimitiveIntArray(Q.project(keyCols, new MatchableTableColumn.ColumnIndexProjection()));
//					String value = StringUtils.join(Q.sort(Q.toString(Q.toList(record.get(indices)))), "/");
//					
//					resultCollector.next(new Pair<TableKeyWithValue, MatchableTableRow>(new TableKeyWithValue(keyCols, value), record));
//				}
				
				// this variant creates links if any two attributes match
//				// create a list of all candidate keys and their subsets
//				Set<Set<MatchableTableColumn>> possibleDeterminants = new HashSet<>();
//				for(MatchableTableColumn[] key : record.getKeys()) {
//					
//					for(MatchableTableColumn col : key) {						
//						possibleDeterminants.add(Q.toSet(col));
//					}
//					
//				}
//				
//				for(Set<MatchableTableColumn> keyCols : possibleDeterminants) {
//					int[] indices = Q.toPrimitiveIntArray(Q.project(keyCols, new MatchableTableColumn.ColumnIndexProjection()));
//					String value = StringUtils.join(Q.sort(Q.toString(Q.toList(record.get(indices)))), "/");
//					
//					if(!value.isEmpty()) {
//						resultCollector.next(new Pair<TableKeyWithValue, MatchableTableRow>(new TableKeyWithValue(keyCols, value), record));
//					}
//				}
				
				// this variant creates links only if two candidate keys match
				for(MatchableTableColumn[] key : record.getKeys()) {
					Set<MatchableTableColumn> keyCols = Q.toSet(key);
					int[] indices = Q.toPrimitiveIntArray(Q.project(keyCols, new MatchableTableColumn.ColumnIndexProjection()));
					String value = StringUtils.join(Q.sort(Q.toString(Q.toList(record.get(indices)))), "/");
					
					resultCollector.next(new Pair<TableKeyWithValue, MatchableTableRow>(new TableKeyWithValue(keyCols, value), record));
				}
	
			}
		};
		
		ResultSet<Pair<TableKeyWithValue, MatchableTableRow>> dataWithBlockingKey = engine.transform(dataset, blockingMapper);
		
		Function<String, Pair<TableKeyWithValue, MatchableTableRow>> joinKeyGenerator = new Function<String, Pair<TableKeyWithValue,MatchableTableRow>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(Pair<TableKeyWithValue, MatchableTableRow> input) {
				return input.getFirst().value;
			}
		};
		
		ResultSet<Pair<Pair<TableKeyWithValue, MatchableTableRow>, Pair<TableKeyWithValue, MatchableTableRow>>> joined = engine.symmetricSelfJoin(dataWithBlockingKey, joinKeyGenerator);
		
		RecordMapper<Pair<Pair<TableKeyWithValue, MatchableTableRow>, Pair<TableKeyWithValue, MatchableTableRow>>, Correspondence<MatchableTableRow, MatchableTableKey>> pairsToCorrespondenceMapper = new RecordMapper<Pair<Pair<TableKeyWithValue,MatchableTableRow>,Pair<TableKeyWithValue,MatchableTableRow>>, Correspondence<MatchableTableRow,MatchableTableKey>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Pair<TableKeyWithValue, MatchableTableRow>, Pair<TableKeyWithValue, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableKey>> resultCollector) {
				
				List<Pair<TableKeyWithValue, MatchableTableRow>> sortedPairs = Q.sort(Q.toList(record.getFirst(), record.getSecond()), new Comparator<Pair<TableKeyWithValue, MatchableTableRow>>() {

					@Override
					public int compare(Pair<TableKeyWithValue, MatchableTableRow> o1,
							Pair<TableKeyWithValue, MatchableTableRow> o2) {
						return Integer.compare(o1.getSecond().getTableId(), o2.getSecond().getTableId());
					}
				});
				
				MatchableTableRow record1 = sortedPairs.get(0).getSecond();
				MatchableTableRow record2 = sortedPairs.get(1).getSecond();
				
				if(record1.getTableId()!=record2.getTableId()) {
				
					TableKeyWithValue keyValue1 = sortedPairs.get(0).getFirst();
					TableKeyWithValue keyValue2 = sortedPairs.get(1).getFirst();
					
					MatchableTableKey key1 = new MatchableTableKey(record1.getTableId(), keyValue1.key);
					MatchableTableKey key2 = new MatchableTableKey(record2.getTableId(), keyValue2.key);
					
					Correspondence<MatchableTableKey, MatchableTableRow> keyCor = 
							new Correspondence<MatchableTableKey, MatchableTableRow>(key1, key2, 1.0, null);
					
					ResultSet<Correspondence<MatchableTableKey, MatchableTableRow>> keyCors = new ResultSet<>();
					keyCors.add(keyCor);
					
					Correspondence<MatchableTableRow, MatchableTableKey> cor = 
							new Correspondence<MatchableTableRow, MatchableTableKey>(record1, record2, 1.0, keyCors);
					
					resultCollector.next(cor);
				}
				
			}
		};
		
		return engine.transform(joined, pairsToCorrespondenceMapper);
	}

}