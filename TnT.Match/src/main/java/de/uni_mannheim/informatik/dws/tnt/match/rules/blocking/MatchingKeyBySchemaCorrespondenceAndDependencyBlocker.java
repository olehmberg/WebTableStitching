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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
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
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Generates matching keys from the given schema correspondences and functional dependencies such that every left-hand side of a functional dependency becomes a matching key if its columns have schema correspondences
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchingKeyBySchemaCorrespondenceAndDependencyBlocker<T> {

	public ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> runBlocking(
			BasicCollection<MatchableTableKey> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableColumn, T>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		// group the FDs with the schema correspondences by table id
		Function<Integer, MatchableTableKey> keyToTableId = new Function<Integer, MatchableTableKey>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableKey input) {
				return input.getTableId();
			}
		};
		Function<Integer, Correspondence<MatchableTableColumn, T>> correspondenceToTableId = new Function<Integer, Correspondence<MatchableTableColumn,T>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableColumn, T> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		RecordMapper<Pair<Iterable<MatchableTableKey>, Iterable<Correspondence<MatchableTableColumn, T>>>, Correspondence<MatchableTableKey, MatchableTableColumn>> resultMapper = new RecordMapper<Pair<Iterable<MatchableTableKey>,Iterable<Correspondence<MatchableTableColumn,T>>>, Correspondence<MatchableTableKey,MatchableTableColumn>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Iterable<MatchableTableKey>, Iterable<Correspondence<MatchableTableColumn, T>>> record,
					DatasetIterator<Correspondence<MatchableTableKey, MatchableTableColumn>> resultCollector) {

				// index the schema correspondences (right table id -> cors)
				Map<Integer, Collection<Correspondence<MatchableTableColumn, T>>> corsByTable = Q.group(record.getSecond(), new Func<Integer, Correspondence<MatchableTableColumn, T>>() {

					@Override
					public Integer invoke(Correspondence<MatchableTableColumn, T> in) {
						return in.getSecondRecord().getTableId();
					}});
				
				// for each LHS
				for(MatchableTableKey lhs : record.getFirst()) {
					
					// create all possible mappings from lhs to another table
					
					// iterate over all mapped tables
					for(Integer rightTblId : corsByTable.keySet()) {
						Collection<Correspondence<MatchableTableColumn, T>> corsForRightTable = corsByTable.get(rightTblId);

						// index the schema correspondences (left id -> right columns)
						Map<Integer, Collection<Correspondence<MatchableTableColumn, T>>> cors = Q.group(corsForRightTable, new Func<Integer, Correspondence<MatchableTableColumn, T>>() {

							@Override
							public Integer invoke(Correspondence<MatchableTableColumn, T> in) {
								return in.getFirstRecord().getColumnIndex();
							}});
						
						Set<MatchableTableColumn> rightColumns = new HashSet<>();
						Set<Correspondence<MatchableTableColumn, T>> usedCorrespondences = new HashSet<>();
						
						// for each attribute in lhs, check if there is a correspondence
						for(MatchableTableColumn c : lhs.getColumns()) {
							
							if(cors.containsKey(c.getColumnIndex())) {
								Correspondence<MatchableTableColumn, T> correspondence = Q.firstOrDefault(cors.get(c.getColumnIndex()));
								
								if(correspondence!=null) {
									rightColumns.add(correspondence.getSecondRecord());
									usedCorrespondences.add(correspondence);
								}
							}
							
						}
						
						// if all attributes in lhs have a correspondence
						if(rightColumns.size()==lhs.getColumns().size()) {
							
							// create a correspondence as output
							MatchableTableKey rightK = new MatchableTableKey(rightTblId, rightColumns);
							
							ResultSet<Correspondence<MatchableTableColumn, MatchableTableKey>> causes = new ResultSet<>();
							
							for(Correspondence<MatchableTableColumn, T> cause : usedCorrespondences) {
								Correspondence<MatchableTableColumn, MatchableTableKey> newC = new Correspondence<MatchableTableColumn, MatchableTableKey>(cause.getFirstRecord(), cause.getSecondRecord(), cause.getSimilarityScore(), null);
							}
							
							Correspondence<MatchableTableKey, MatchableTableColumn> cor = new Correspondence<MatchableTableKey, MatchableTableColumn>(lhs, rightK, 1.0, causes);

							resultCollector.next(cor);
						}
						
						
					}
					
				}
				
			}
		};
		return engine.coGroup(dataset, schemaCorrespondences, keyToTableId, correspondenceToTableId, resultMapper);
	}
	
}
