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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

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
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

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
	
	public Processable<Correspondence<MatchableTableColumn, Matchable>> runBlocking(
			DataSet<MatchableTableDeterminant, MatchableTableColumn> dataset,
			boolean isSymmetric,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		// join keys and correspondences by table combination
		
		
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
		Function<Integer, Correspondence<MatchableTableColumn, Matchable>> corToLeftTableId = new Function<Integer, Correspondence<MatchableTableColumn,Matchable>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableColumn, Matchable> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		Processable<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>> firstJoin = dataset.join(schemaCorrespondences, keyToTableId, corToLeftTableId);
		
		Function<Integer, Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>> joinToRightTableId = new Function<Integer, Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>> input) {
				return input.getSecond().getSecondRecord().getTableId();
			}
		};
		Processable<Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>> secondJoin = firstJoin.join(dataset, joinToRightTableId, keyToTableId);
		
		// group by table combination
		
		RecordKeyValueMapper<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>> groupByTableCombination = new RecordKeyValueMapper<List<Integer>, Pair<Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>, Pair<Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(
					Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant> record,
					DataIterator<Pair<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>>> resultCollector) {
				resultCollector.next(new Pair<List<Integer>, Pair<Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>>(Q.toList(record.getFirst().getFirst().getTableId(), record.getSecond().getTableId()), record));
			}
		};
		Processable<Group<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>>> grouped = secondJoin.group(groupByTableCombination);
		
		// filter
		
		RecordMapper<Group<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>>, Correspondence<MatchableTableColumn, Matchable>> transformation = new RecordMapper<Group<List<Integer>,Pair<Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>>, Correspondence<MatchableTableColumn,Matchable>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Group<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>> record,
					DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) {

				Collection<MatchableTableDeterminant> keys1 = new HashSet<>();
				Collection<MatchableTableDeterminant> keys2 = new HashSet<>();
				Collection<Correspondence<MatchableTableColumn, Matchable>> cors = new HashSet<>();
				Collection<MatchableTableColumn> mappedColumns1 = new HashSet<>();
				Collection<MatchableTableColumn> mappedColumns2 = new HashSet<>();
				
				for(Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant> p : record.getRecords().get()) {
					
					keys1.add(p.getFirst().getFirst());
					keys2.add(p.getSecond());
					
					Correspondence<MatchableTableColumn, Matchable> cor = p.getFirst().getSecond();
					
					cors.add(cor);
					
					mappedColumns1.add(cor.getFirstRecord());
					mappedColumns2.add(cor.getSecondRecord());
					
				}
				
				MatchableTableDeterminant key1 = null;
				boolean key1Match = false;
				for(MatchableTableDeterminant k : keys1) {
					if(mappedColumns1.containsAll(k.getColumns())) {
						key1Match=true;
						key1 = k;
						break;
					}
				}
				
				MatchableTableDeterminant key2 = null;
				boolean key2Match = false;
				for(MatchableTableDeterminant k : keys2) {
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
					for(Correspondence<MatchableTableColumn, Matchable> cor : cors) {
						resultCollector.next(cor);
					}
				}
			}
		};
		return grouped.map(transformation);
	}

}
