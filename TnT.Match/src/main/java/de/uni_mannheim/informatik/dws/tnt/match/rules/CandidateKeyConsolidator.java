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
package de.uni_mannheim.informatik.dws.tnt.match.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.P;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
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
public class CandidateKeyConsolidator<T> {

	private boolean removeSubsets  = true;
	
	public CandidateKeyConsolidator() {
		
	}
	
	public CandidateKeyConsolidator(boolean removeSubsets) {
		this.removeSubsets = removeSubsets;
	}
	
	public ResultSet<MatchableTableKey> run(BasicCollection<MatchableTableKey> data, ResultSet<Correspondence<MatchableTableKey, T>> correspondences, DataProcessingEngine engine) {
		
		// transform the correspondences to keys
		RecordMapper<Correspondence<MatchableTableKey, T>, MatchableTableKey> correspondenceToKeys = new RecordMapper<Correspondence<MatchableTableKey,T>, MatchableTableKey>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableKey, T> record,
					DatasetIterator<MatchableTableKey> resultCollector) {
				
				resultCollector.next(record.getFirstRecord());
				
				if(record.getSecondRecord().getTableId()>=0) {
					resultCollector.next(record.getSecondRecord());
				}
				
			}
		};
		ResultSet<MatchableTableKey> correspondenceKeys = engine.transform(correspondences, correspondenceToKeys);
		
		ResultSet<MatchableTableKey> allKeys = engine.append(data, correspondenceKeys);
		
		// group the original keys and the ones in the correspondences by table id		
		RecordKeyValueMapper<Integer, MatchableTableKey, MatchableTableKey> dataToGroupingKey = new RecordKeyValueMapper<Integer, MatchableTableKey, MatchableTableKey>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(MatchableTableKey record, DatasetIterator<Pair<Integer, MatchableTableKey>> resultCollector) {
				resultCollector.next(new Pair<Integer, MatchableTableKey>(record.getTableId(), record));
			}
		};
		ResultSet<Group<Integer, MatchableTableKey>> grouped = engine.groupRecords(allKeys, dataToGroupingKey);
		
		RecordMapper<Group<Integer, MatchableTableKey>, MatchableTableKey> consolidation = new RecordMapper<Group<Integer,MatchableTableKey>, MatchableTableKey>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Integer, MatchableTableKey> record,
					DatasetIterator<MatchableTableKey> resultCollector) {
				
				Set<MatchableTableKey> keys = new HashSet<>();
				
				// merge and de-duplicate keys
				for(MatchableTableKey k : record.getRecords().get()) {
					keys.add(k);
				}
				
				
				if(removeSubsets) {
	//				// keep all previously generated keys, otherwise we loose record links during the next record blocking step
	//				for(MatchableTableKey k : keys) {
	//					resultCollector.next(k);
	//				}
					
					// remove keys that are contained in another key (and hence underestimated)
					List<MatchableTableKey> keysToCheck = new ArrayList<MatchableTableKey>(keys);
					for(MatchableTableKey k : keysToCheck) {
						keys.remove(k);
						
						if(!Q.any(Q.project(keys, new MatchableTableKey.ColumnsProjection()), new P.ContainsAll<MatchableTableColumn>(k.getColumns()))) {
							// there is no larger key in keys that contains all elements of k, so k is maximal
							resultCollector.next(k);
							
							// k is a larger key, so we keep it to check the remaining keys
							keys.add(k);
						}
					}
				} else {
					for(MatchableTableKey k : keys) {
						resultCollector.next(k);
					}
				}
			}
		};
		
		ResultSet<MatchableTableKey> consolidated = engine.transform(grouped, consolidation);
		
		return consolidated;
	}
	
}
