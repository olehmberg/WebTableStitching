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
package de.uni_mannheim.informatik.dws.tnt.match.rules.refiner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CorrespondenceInverter {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableKey>> aggregate(ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> correspondences,
			DataProcessingEngine proc) {
	
		// group schema correspondences by table combination
		RecordKeyValueMapper<String, Correspondence<MatchableTableKey, MatchableTableColumn>, Correspondence<MatchableTableKey, MatchableTableColumn>> groupByTableCombination = new RecordKeyValueMapper<String, Correspondence<MatchableTableKey, MatchableTableColumn>, Correspondence<MatchableTableKey, MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableKey, MatchableTableColumn> record,
					DatasetIterator<Pair<String, Correspondence<MatchableTableKey, MatchableTableColumn>>> resultCollector) {
				
				String key = record.getFirstRecord().getTableId() + "/" + record.getSecondRecord().getTableId();
				resultCollector.next(new Pair<String, Correspondence<MatchableTableKey, MatchableTableColumn>>(key, record));
				
			}
		};
		
		
		ResultSet<Group<String, Correspondence<MatchableTableKey, MatchableTableColumn>>> grouped = proc.groupRecords(correspondences, groupByTableCombination);
		
		RecordMapper<Group<String, Correspondence<MatchableTableKey, MatchableTableColumn>>, Correspondence<MatchableTableColumn, MatchableTableKey>> propagateKeys = new RecordMapper<Group<String,Correspondence<MatchableTableKey, MatchableTableColumn>>, Correspondence<MatchableTableColumn, MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<String, Correspondence<MatchableTableKey, MatchableTableColumn>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableKey>> resultCollector) {
				
				HashSet<Correspondence<MatchableTableColumn, MatchableTableKey>> schemaCors = new HashSet<>();
				
				// collect all the keys, but remove their causal correspondences
				ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> reducedKeyCors = new ResultSet<>();
				
				
				for(Correspondence<MatchableTableKey, MatchableTableColumn> keyCor : record.getRecords().get()) {
					schemaCors.addAll(keyCor.getCausalCorrespondences().get());
					
					reducedKeyCors.add(new Correspondence<MatchableTableKey, MatchableTableColumn>(keyCor.getFirstRecord(), keyCor.getSecondRecord(), keyCor.getSimilarityScore(), null));
				}
				
				for(Correspondence<MatchableTableColumn, MatchableTableKey> cor : schemaCors) {
					resultCollector.next(new Correspondence<MatchableTableColumn, MatchableTableKey>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), reducedKeyCors));
				}
			}
		};
		
		return proc.transform(grouped, propagateKeys);
		
	}
}
