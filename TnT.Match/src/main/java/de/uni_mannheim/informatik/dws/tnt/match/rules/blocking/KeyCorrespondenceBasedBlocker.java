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
public class KeyCorrespondenceBasedBlocker {

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
			ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> schemaCorrespondences,
			DataProcessingEngine engine) {

		// create the values for all keys that are in the correspondences
		
		// first, transform correspondences to a dataset of keys
		
		RecordMapper<Correspondence<MatchableTableKey, MatchableTableColumn>, MatchableTableKey> keyTransformation =  new RecordMapper<Correspondence<MatchableTableKey,MatchableTableColumn>, MatchableTableKey>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableKey, MatchableTableColumn> record,
					DatasetIterator<MatchableTableKey> resultCollector) {
				resultCollector.next(record.getFirstRecord());
				if(record.getSecondRecord()!=null) {
					resultCollector.next(record.getSecondRecord());
				}
			}
		};
		ResultSet<MatchableTableKey> keysToMatch = engine.transform(schemaCorrespondences, keyTransformation);
		keysToMatch.deduplicate();
		System.out.println(String.format("%d matching keys", keysToMatch.size()));
		
		// join all records with the keys 
		Function<Integer, MatchableTableRow> recordToTableId = new Function<Integer, MatchableTableRow>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableRow input) {
				return input.getTableId();
			}
		};
		
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
		ResultSet<Pair<MatchableTableRow, MatchableTableKey>> recordsWithKeys = engine.joinMixedTypes(dataset, keysToMatch, recordToTableId , keyToTableId);
		System.out.println(String.format("%d record/key combinations", recordsWithKeys.size()));
		
		// transform join result to records with key values
		
		RecordMapper<Pair<MatchableTableRow, MatchableTableKey>, Pair<TableKeyWithValue, MatchableTableRow>> recordsWithKeysToValues = new RecordMapper<Pair<MatchableTableRow,MatchableTableKey>, Pair<TableKeyWithValue,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<MatchableTableRow, MatchableTableKey> record,
					DatasetIterator<Pair<TableKeyWithValue, MatchableTableRow>> resultCollector) {
				
				MatchableTableRow row = record.getFirst();
				MatchableTableKey key = record.getSecond();
				
				Set<MatchableTableColumn> keyCols = key.getColumns();
				int[] indices = Q.toPrimitiveIntArray(Q.project(keyCols, new MatchableTableColumn.ColumnIndexProjection()));
				String value = StringUtils.join(Q.sort(Q.toString(Q.toList(row.get(indices)))), "/");
				
				resultCollector.next(new Pair<TableKeyWithValue, MatchableTableRow>(new TableKeyWithValue(keyCols, value), row));
				
			}
		};
		ResultSet<Pair<TableKeyWithValue, MatchableTableRow>> dataWithBlockingKey = engine.transform(recordsWithKeys, recordsWithKeysToValues );
		
		
		
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
		System.out.println(String.format("%d record combinations", joined.size()));
		
		
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