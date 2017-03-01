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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.plaf.basic.BasicSplitPaneUI.KeyboardUpLeftHandler;

import check_if_useful.MatchingKeyGenerator;
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
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KeyPropagationAggregator {


	public ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> aggregate(ResultSet<Correspondence<MatchableTableColumn, MatchableTableKey>> correspondences,
			DataProcessingEngine proc) {
	
		// group schema correspondences by table combination
		RecordKeyValueMapper<String, Correspondence<MatchableTableColumn, MatchableTableKey>, Correspondence<MatchableTableColumn, MatchableTableKey>> groupByTableCombination = new RecordKeyValueMapper<String, Correspondence<MatchableTableColumn,MatchableTableKey>, Correspondence<MatchableTableColumn, MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableColumn, MatchableTableKey> record,
					DatasetIterator<Pair<String, Correspondence<MatchableTableColumn, MatchableTableKey>>> resultCollector) {
				
				String key = record.getFirstRecord().getTableId() + "/" + record.getSecondRecord().getTableId();
				resultCollector.next(new Pair<String, Correspondence<MatchableTableColumn,MatchableTableKey>>(key, record));
				
			}
		};
		
		
		ResultSet<Group<String, Correspondence<MatchableTableColumn, MatchableTableKey>>> grouped = proc.groupRecords(correspondences, groupByTableCombination);
		
		RecordMapper<Group<String, Correspondence<MatchableTableColumn, MatchableTableKey>>, Correspondence<MatchableTableKey, MatchableTableColumn>> propagateKeys = new RecordMapper<Group<String,Correspondence<MatchableTableColumn,MatchableTableKey>>, Correspondence<MatchableTableKey,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<String, Correspondence<MatchableTableColumn, MatchableTableKey>> record,
					DatasetIterator<Correspondence<MatchableTableKey, MatchableTableColumn>> resultCollector) {
				
				// determine candidate keys from schema correspondences
				HashSet<MatchableTableColumn> schema1Set = new HashSet<>();
				HashSet<MatchableTableColumn> schema2Set = new HashSet<>();
				
				HashSet<Set<MatchableTableColumn>> keys1 = new HashSet<>();
				HashSet<Set<MatchableTableColumn>> keys2 = new HashSet<>();
				
				for(Correspondence<MatchableTableColumn, MatchableTableKey> schemaCor : record.getRecords().get()) {
					schema1Set.add(schemaCor.getFirstRecord());
					schema2Set.add(schemaCor.getSecondRecord());
					
					for(Correspondence<MatchableTableKey, MatchableTableColumn> keyCor : schemaCor.getCausalCorrespondences().get()) {
						keys1.add(keyCor.getFirstRecord().getColumns());
						keys2.add(keyCor.getSecondRecord().getColumns());
					}
				}
				
				Map<Integer, MatchableTableColumn> schema1 = Q.map(schema1Set, new MatchableTableColumn.ColumnIndexProjection());
				Map<Integer, MatchableTableColumn> schema2 = Q.map(schema2Set, new MatchableTableColumn.ColumnIndexProjection());
//				List<MatchableTableColumn> schema1 = Q.sort(schema1Set, new MatchableTableColumn.ColumnIndexComparator());
//				List<MatchableTableColumn> schema2 = Q.sort(schema2Set, new MatchableTableColumn.ColumnIndexComparator());
				
				int table1Id = Q.firstOrDefault(schema1.values()).getTableId();
				int table2Id = Q.firstOrDefault(schema2.values()).getTableId();
				
				MatchingKeyGenerator mkg = new MatchingKeyGenerator();
				Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> propagated = mkg.generateAllMatchingKeysFromCorrespondences(table1Id, table2Id, schema1, schema2, keys1, keys2, record.getRecords(), 0.0);
				
				if(propagated!=null) {
					for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : propagated) {
						resultCollector.next(cor);
					}
				} else {
//					System.out.println(String.format("No connection between tables #%d and #%d", table1Id, table2Id));
				}
			}
		};
		
		return proc.transform(grouped, propagateKeys);
	}
	
}
