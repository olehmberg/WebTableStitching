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

import java.util.HashMap;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
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
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SynonymBasedSchemaBlocker {

	private ResultSet<Set<String>> synonyms;
	
	/**
	 * 
	 */
	public SynonymBasedSchemaBlocker(ResultSet<Set<String>> synonyms) {
		this.synonyms = synonyms;
	}
	
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> runBlocking(
			DataSet<MatchableTableColumn, MatchableTableColumn> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		// create join keys for each synonym cluster
		final HashMap<String, String> clusterMap = new HashMap<>();
		int clusterIndex = 0;
		for(Set<String> cluster : synonyms.get()) {
			String clusterKey = Integer.toString(clusterIndex);
			for(String synonym : cluster) {
				clusterMap.put(synonym, clusterKey);
			}
			clusterIndex++;
		}
		
		Function<String, MatchableTableColumn> columnToSynonym = new Function<String, MatchableTableColumn>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(MatchableTableColumn input) {
				if(input.getHeader().equals("null")) {
					return null;
				} else {
					// use the cluster key as blocking key
					return clusterMap.get(input.getHeader());
				}
			}
		};
		
		ResultSet<Pair<MatchableTableColumn, MatchableTableColumn>> joined = engine.symmetricSelfJoin(dataset, columnToSynonym);
		
		RecordKeyValueMapper<String, Pair<MatchableTableColumn, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>> deduplicate = new RecordKeyValueMapper<String, Pair<MatchableTableColumn,MatchableTableColumn>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<MatchableTableColumn, MatchableTableColumn> record,
					DatasetIterator<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {
				
				if(record.getFirst().getTableId()!=record.getSecond().getTableId() && clusterMap.get(record.getFirst().getHeader())!=null) {
					
					MatchableTableColumn c1 = record.getFirst();
					MatchableTableColumn c2 = record.getSecond();
					
					if(c1.getTableId()>c2.getTableId()) {
						MatchableTableColumn tmp = c1;
						c1 = c2;
						c2 = tmp;
					}
					
					String key = c1.getIdentifier() + "/" + c2.getIdentifier();
					//Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, 0.1, null);
					Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, 1.0, null);
					
					resultCollector.next(new Pair<String, Correspondence<MatchableTableColumn,MatchableTableRow>>(key, cor));
					
				}
				
				
			}
		};
		
		ResultSet<Group<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> grouped = engine.groupRecords(joined, deduplicate);
		
		RecordMapper<Group<String, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn, MatchableTableRow>> joinedToCorrespondences = new RecordMapper<Group<String, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<String, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {

				if(record.getRecords().size()>1) {
					System.out.println("duplicate");
				}
				
				if(record.getRecords().size()>0) {
					resultCollector.next(record.getRecords().get().iterator().next());
				}
				
			}				
		};
		
		return engine.transform(grouped, joinedToCorrespondences);
	}
	
}
