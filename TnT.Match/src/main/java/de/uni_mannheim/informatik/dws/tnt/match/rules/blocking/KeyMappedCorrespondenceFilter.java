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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

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
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * Removes all correspondences between tables where no candidate key is mapped
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KeyMappedCorrespondenceFilter {
	
	private boolean mustContainStringColumn = false;
	
	public KeyMappedCorrespondenceFilter(boolean mustContainStringColumn) {
		this.mustContainStringColumn = mustContainStringColumn;
	}
	
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> runBlocking(
			DataSet<MatchableTableKey, MatchableTableColumn> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		// join keys and correspondences by table combination
		
		
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
		Function<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>> corToLeftTableId = new Function<Integer, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableColumn, MatchableTableRow> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		ResultSet<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>> firstJoin = engine.joinMixedTypes(dataset, schemaCorrespondences, keyToTableId, corToLeftTableId);
		
		Function<Integer, Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>> joinToRightTableId = new Function<Integer, Pair<MatchableTableKey,Correspondence<MatchableTableColumn,MatchableTableRow>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>> input) {
				return input.getSecond().getSecondRecord().getTableId();
			}
		};
		ResultSet<Pair<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>, MatchableTableKey>> secondJoin = engine.joinMixedTypes(firstJoin, dataset, joinToRightTableId, keyToTableId);
		
		// group by table combination
		
		RecordKeyValueMapper<List<Integer>, Pair<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>, MatchableTableKey>, Pair<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>, MatchableTableKey>> groupByTableCombination = new RecordKeyValueMapper<List<Integer>, Pair<Pair<MatchableTableKey,Correspondence<MatchableTableColumn,MatchableTableRow>>,MatchableTableKey>, Pair<Pair<MatchableTableKey,Correspondence<MatchableTableColumn,MatchableTableRow>>,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>, MatchableTableKey> record,
					DatasetIterator<Pair<List<Integer>, Pair<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>, MatchableTableKey>>> resultCollector) {
				resultCollector.next(new Pair<List<Integer>, Pair<Pair<MatchableTableKey,Correspondence<MatchableTableColumn,MatchableTableRow>>,MatchableTableKey>>(Q.toList(record.getFirst().getFirst().getTableId(), record.getSecond().getTableId()), record));
			}
		};
		ResultSet<Group<List<Integer>, Pair<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>, MatchableTableKey>>> grouped = engine.groupRecords(secondJoin, groupByTableCombination);
		
		// filter
		
		RecordMapper<Group<List<Integer>, Pair<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>, MatchableTableKey>>, Correspondence<MatchableTableColumn, MatchableTableRow>> transformation = new RecordMapper<Group<List<Integer>,Pair<Pair<MatchableTableKey,Correspondence<MatchableTableColumn,MatchableTableRow>>,MatchableTableKey>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Group<List<Integer>, Pair<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>, MatchableTableKey>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {

				Collection<MatchableTableKey> keys1 = new HashSet<>();
				Collection<MatchableTableKey> keys2 = new HashSet<>();
				Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> cors = new HashSet<>();
				Collection<MatchableTableColumn> mappedColumns1 = new HashSet<>();
				Collection<MatchableTableColumn> mappedColumns2 = new HashSet<>();
				
				for(Pair<Pair<MatchableTableKey, Correspondence<MatchableTableColumn, MatchableTableRow>>, MatchableTableKey> p : record.getRecords().get()) {
					
					keys1.add(p.getFirst().getFirst());
					keys2.add(p.getSecond());
					
					Correspondence<MatchableTableColumn, MatchableTableRow> cor = p.getFirst().getSecond();
					
					cors.add(cor);
					
					mappedColumns1.add(cor.getFirstRecord());
					mappedColumns2.add(cor.getSecondRecord());
					
				}
				
				MatchableTableKey key1 = null;
				boolean key1Match = false;
				for(MatchableTableKey k : keys1) {
					if(mappedColumns1.containsAll(k.getColumns())) {
						key1Match=true;
						key1 = k;
						break;
					}
				}
				
				MatchableTableKey key2 = null;
				boolean key2Match = false;
				for(MatchableTableKey k : keys2) {
					if(mappedColumns2.containsAll(k.getColumns())) {
						key2Match=true;
						key2 = k;
						break;
					}
				}
				
				boolean otherConditions = true;
				if(mustContainStringColumn) {
//					otherConditions &= key1!=null&&Q.any(key1.getColumns(), new MatchableTableColumn.IsStringColumnProjection())
//							|| key2!=null&&Q.any(key2.getColumns(), new MatchableTableColumn.IsStringColumnProjection());
					otherConditions &= key1Match&&Q.any(key1.getColumns(), new MatchableTableColumn.IsStringColumnProjection())
							|| key2Match&&Q.any(key2.getColumns(), new MatchableTableColumn.IsStringColumnProjection());
				}
				
				if( (key1Match || key2Match) && otherConditions) {
					for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : cors) {
						resultCollector.next(cor);
					}
				}
			}
		};
		return engine.transform(grouped, transformation);
	}

}
