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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import check_if_useful.MatchingKeyGenerator;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils2;
import de.uni_mannheim.informatik.dws.t2k.utils.query.P;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaToKeyKeyAggregator {


	public ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> aggregate(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences,
			DataProcessingEngine proc) {
	
		// group schema correspondences by table combination
		RecordKeyValueMapper<String, Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>> groupByTableCombination = new RecordKeyValueMapper<String, Correspondence<MatchableTableColumn,MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableColumn, MatchableTableRow> record,
					DatasetIterator<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {
				
				String key = record.getFirstRecord().getTableId() + "/" + record.getSecondRecord().getTableId();
				resultCollector.next(new Pair<String, Correspondence<MatchableTableColumn,MatchableTableRow>>(key, record));
				
			}
		};
		
		
		ResultSet<Group<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> grouped = proc.groupRecords(correspondences, groupByTableCombination);
		
		RecordMapper<Group<String, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableKey, MatchableTableColumn>> propagateKeys = new RecordMapper<Group<String,Correspondence<MatchableTableColumn,MatchableTableRow>>, Correspondence<MatchableTableKey,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<String, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableKey, MatchableTableColumn>> resultCollector) {
				
				// determine candidate keys from an instance correspondence
				Correspondence<MatchableTableColumn, MatchableTableRow> schema = Q.firstOrDefault(record.getRecords().get());
				Correspondence<MatchableTableRow, MatchableTableColumn> instance = Q.firstOrDefault(schema.getCausalCorrespondences().get());
				 
				MatchableTableRow record1 = instance.getFirstRecord();
				MatchableTableRow record2 = instance.getSecondRecord();
				
				if(record1.getTableId()>record2.getTableId()) {
					System.out.println("Wrong direction!");
				}
				
				List<Set<MatchableTableColumn>> keys1 = new ArrayList<>(record1.getKeys().length);
				for(MatchableTableColumn[] k : record1.getKeys()) {
					keys1.add(Q.toSet(k));
				}
				List<Set<MatchableTableColumn>> keys2 = new ArrayList<>(record2.getKeys().length);
				for(MatchableTableColumn[] k : record2.getKeys()) {
					keys2.add(Q.toSet(k));
				}
				
				List<MatchableTableColumn> schema1 = Q.toList(record1.getSchema());
				List<MatchableTableColumn> schema2 = Q.toList(record2.getSchema());
				
				// transform schema correspondences
				ResultSet<Correspondence<MatchableTableColumn, MatchableTableKey>> schemaCors = new ResultSet<>();
				// also index schema correspondences for later retrieval
				Map<Integer, Map<Integer, Correspondence<MatchableTableColumn, MatchableTableKey>>> schemaCorIndex = new HashMap<>();
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : record.getRecords().get()) {
					Correspondence<MatchableTableColumn, MatchableTableKey> newCor = new Correspondence<MatchableTableColumn, MatchableTableKey>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), null);
					schemaCors.add(newCor);
					MapUtils2.put(schemaCorIndex, cor.getFirstRecord().getColumnIndex(), cor.getSecondRecord().getColumnIndex(), newCor);
				}
				
				
				
				
				
				double minSimilarity = 0.0;
				Set<Correspondence<MatchableTableKey, MatchableTableColumn>> keys = new HashSet<>();
				
				// convert correspondences to column map
				Map<Integer, Integer> columnCorrespondenceMap = new HashMap<>();
				Map<Integer, Integer> columnCorrespondenceMapReverse = new HashMap<>();
				for(Correspondence<MatchableTableColumn, MatchableTableKey> cor : schemaCors.get()) {
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
				for(Set<MatchableTableColumn> t1Key : keys1) {
					Set<MatchableTableColumn> correspondingColumns = new HashSet<>();
					
					for(MatchableTableColumn c : t1Key) {
						if(columnCorrespondenceMap.containsKey(c.getColumnIndex())) {
							correspondingColumns.add(schema2.get(columnCorrespondenceMap.get(c.getColumnIndex())));
						}
					}
					
					boolean rightKeyMatch = false;
					for(Set<MatchableTableColumn> rightKey : keys2) {
						if(correspondingColumns.containsAll(rightKey)) {
							rightKeyMatch=true;
							break;
						}
					}
					
					// make sure that there is a corresponding column for each column contained in the left key and that a right key is completely contained in the corresponding columns
					if(correspondingColumns.size()==t1Key.size() && rightKeyMatch) {			
						joinKeyT1 = t1Key;
						joinKeyT2 = correspondingColumns;
						
						// create the schema correspondence for this key match
						ResultSet<Correspondence<MatchableTableColumn, MatchableTableKey>> causes = new ResultSet<>();
						for(MatchableTableColumn col1 : joinKeyT1) {
							int correspondingColumn2 = columnCorrespondenceMap.get(col1.getColumnIndex());
							
							causes.add(schemaCorIndex.get(col1.getColumnIndex()).get(correspondingColumn2));
						}
						
						keys.add(new Correspondence<MatchableTableKey, MatchableTableColumn>(
								new MatchableTableKey(record1.getTableId(), joinKeyT1), 
								new MatchableTableKey(record2.getTableId(), joinKeyT2), 
								1.0, 
								causes));
					}
				}
				
				// do the same for the other direction
				for(Set<MatchableTableColumn> t2Key : keys2) {
					Set<MatchableTableColumn> correspondingColumns = new HashSet<>();
					
					for(MatchableTableColumn c : t2Key) {
						if(columnCorrespondenceMapReverse.containsKey(c.getColumnIndex())) {
							correspondingColumns.add(schema1.get(columnCorrespondenceMapReverse.get(c.getColumnIndex())));
						}
					}
					
					boolean leftKeyMatch = false;
					for(Set<MatchableTableColumn> leftKey : keys1) {
						if(correspondingColumns.containsAll(leftKey)) {
							leftKeyMatch=true;
							break;
						}
					}
					
					// create the schema correspondence for this key match
					ResultSet<Correspondence<MatchableTableColumn, MatchableTableKey>> causes = new ResultSet<>();
					for(MatchableTableColumn col1 : joinKeyT1) {
						int correspondingColumn2 = columnCorrespondenceMap.get(col1.getColumnIndex());
						
						causes.add(schemaCorIndex.get(col1.getColumnIndex()).get(correspondingColumn2));
					}
					
					// make sure that there is a corresponding column for each column contained in the key
					if(correspondingColumns.size()==t2Key.size() && leftKeyMatch) {			
						joinKeyT2 = t2Key;
						joinKeyT1 = correspondingColumns;
						
						keys.add(new Correspondence<MatchableTableKey, MatchableTableColumn>(
								new MatchableTableKey(record1.getTableId(), joinKeyT1), 
								new MatchableTableKey(record2.getTableId(), joinKeyT2), 
								1.0, 
								causes));
					}
				}
				
				if(keys.size()>0) {
					for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : keys) {
						resultCollector.next(cor);
					}
				} else {
//					System.out.println(String.format("No connection between tables #%d and #%d", record1.getTableId(), record2.getTableId()));
				}
			}
		};
		
		return proc.transform(grouped, propagateKeys);
	}
	
}
