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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.determinants;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.utils.query.P;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * Merge a collection of {@link MatchableTableDeterminant}s with correspondences between {@link MatchableTableDeterminant}s to create a consolidated collection of {@link MatchableTableDeterminant}s.
 * The consolidated collection contains only maximal {@link MatchableTableDeterminant}s, so no subsets are included. 
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CandidateKeyConsolidator<T extends Matchable> {

	private boolean removeSubsets  = true;
	
	public CandidateKeyConsolidator() {
		
	}
	
	public CandidateKeyConsolidator(boolean removeSubsets) {
		this.removeSubsets = removeSubsets;
	}
	
	public Processable<MatchableTableDeterminant> run(Processable<MatchableTableDeterminant> data, Processable<Correspondence<MatchableTableDeterminant, T>> correspondences) {
		
		// transform the correspondences to keys
		RecordMapper<Correspondence<MatchableTableDeterminant, T>, MatchableTableDeterminant> correspondenceToKeys = new RecordMapper<Correspondence<MatchableTableDeterminant,T>, MatchableTableDeterminant>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableDeterminant, T> record,
					DataIterator<MatchableTableDeterminant> resultCollector) {
				
				resultCollector.next(record.getFirstRecord());
				
				if(record.getSecondRecord().getTableId()>=0) {
					resultCollector.next(record.getSecondRecord());
				}
				
			}
		};
		Processable<MatchableTableDeterminant> correspondenceKeys = correspondences.transform(correspondenceToKeys);
		
		Processable<MatchableTableDeterminant> allKeys = data.append(correspondenceKeys);
		
		// group the original keys and the ones in the correspondences by table id		
		RecordKeyValueMapper<Integer, MatchableTableDeterminant, MatchableTableDeterminant> dataToGroupingKey = new RecordKeyValueMapper<Integer, MatchableTableDeterminant, MatchableTableDeterminant>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(MatchableTableDeterminant record, DataIterator<Pair<Integer, MatchableTableDeterminant>> resultCollector) {
				resultCollector.next(new Pair<Integer, MatchableTableDeterminant>(record.getTableId(), record));
			}
		};
		Processable<Group<Integer, MatchableTableDeterminant>> grouped = allKeys.groupRecords(dataToGroupingKey);
		
		RecordMapper<Group<Integer, MatchableTableDeterminant>, MatchableTableDeterminant> consolidation = new RecordMapper<Group<Integer,MatchableTableDeterminant>, MatchableTableDeterminant>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Integer, MatchableTableDeterminant> record,
					DataIterator<MatchableTableDeterminant> resultCollector) {
				
				Set<MatchableTableDeterminant> keys = new HashSet<>();
				
				// merge and de-duplicate keys
				for(MatchableTableDeterminant k : record.getRecords().get()) {
					keys.add(k);
				}
				
				
				if(removeSubsets) {
	//				// keep all previously generated keys, otherwise we loose record links during the next record blocking step
	//				for(MatchableTableKey k : keys) {
	//					resultCollector.next(k);
	//				}
					
					// remove keys that are contained in another key (and hence underestimated)
					List<MatchableTableDeterminant> keysToCheck = new ArrayList<MatchableTableDeterminant>(keys);
					for(MatchableTableDeterminant k : keysToCheck) {
						keys.remove(k);
						
						if(!Q.any(Q.project(keys, new MatchableTableDeterminant.ColumnsProjection()), new P.ContainsAll<MatchableTableColumn>(k.getColumns()))) {
							// there is no larger key in keys that contains all elements of k, so k is maximal
							resultCollector.next(k);
							
							// k is a larger key, so we keep it to check the remaining keys
							keys.add(k);
						}
					}
				} else {
					for(MatchableTableDeterminant k : keys) {
						resultCollector.next(k);
					}
				}
			}
		};
		
		Processable<MatchableTableDeterminant> consolidated = grouped.transform(consolidation);
		
		return consolidated;
	}
	
}
