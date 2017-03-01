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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Aggregates the schema correspondences to a single column combination (a 'key') that can be used to create record links
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaToColumnCombinationAggregator {
	
	private int minColumns = 1;
	
	public SchemaToColumnCombinationAggregator(int minColumns) {
		this.minColumns = minColumns;
	}

	public ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> aggregate(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences,
			DataProcessingEngine proc) {
		
		// group schema correspondences by table combination
		
		RecordKeyValueMapper<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>> groupByTables = new RecordKeyValueMapper<List<Integer>, Correspondence<MatchableTableColumn,MatchableTableRow>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableColumn, MatchableTableRow> record,
					DatasetIterator<Pair<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {
				
//				if(record.getFirstRecord().getTableId()>record.getSecondRecord().getTableId()) {
//					System.out.println("Wrong Direction!");
//				}
				
				resultCollector.next(new Pair<>(Q.toList(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				
			}
		};
		ResultSet<Group<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>> grouped = proc.groupRecords(correspondences, groupByTables);
		
		// transform to key correspondences
		
		RecordMapper<Group<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableKey, MatchableTableColumn>> transformation = new RecordMapper<Group<List<Integer>,Correspondence<MatchableTableColumn,MatchableTableRow>>, Correspondence<MatchableTableKey,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableKey, MatchableTableColumn>> resultCollector) {
				
				Set<MatchableTableColumn> cols1 = new HashSet<>();
				Set<MatchableTableColumn> cols2 = new HashSet<>();
				
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : record.getRecords().get()) {
					cols1.add(cor.getFirstRecord());
					cols2.add(cor.getSecondRecord());
				}
				
				MatchableTableKey k1 = new MatchableTableKey(record.getKey().get(0), cols1);
				MatchableTableKey k2 = new MatchableTableKey(record.getKey().get(1), cols2);
				
				if(k1.getTableId()>k2.getTableId()) {
					MatchableTableKey tmp = k1;
					k1 = k2;
					k2 = tmp;
				}
				
				if(cols1.size()>=minColumns) {
					resultCollector.next(new Correspondence<MatchableTableKey, MatchableTableColumn>(k1, k2, 1.0, null));
				}
			}
		};
		ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> result = proc.transform(grouped, transformation);
		
		result.deduplicate();
		
		return result;
	}
	
}
