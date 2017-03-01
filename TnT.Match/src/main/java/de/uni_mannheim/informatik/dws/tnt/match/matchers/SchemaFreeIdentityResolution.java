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
package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRowWithKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.aggregators.CountAggregator;

/**
 * 
 * deprecated, use KeyBasedBlocker & WebTablesKeyBlockingKeyGenerator instead
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
@Deprecated
public class SchemaFreeIdentityResolution {

	private HashSet<String> ambiguousValues;
	/**
	 * @return the ambiguousValues
	 */
	public HashSet<String> getAmbiguousValues() {
		return ambiguousValues;
	}
	/**
	 * @param ambiguousValues the ambiguousValues to set
	 */
	public void setAmbiguousValues(HashSet<String> ambiguousValues) {
		this.ambiguousValues = ambiguousValues;
	}
	
	public ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> run(WebTables web, DataProcessingEngine proc, final HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys) {
		//TODO if we use MatchableTableColumn instead of Integer for tableToCandidateKeys, we can generate correspondences instead of Pairs as result
		
	   	/***********************************************
    	 * Schema-less Identity Resolution
    	 ***********************************************/
    	
    	//TODO for execution on full corpus, add site identifier to grouping keys!
    	
    	// Ambiguity avoidance: find all values that appear in multiple candidate key columns and ignore them
    	ResultSet<Pair<String, Integer>> ambiguousValues = proc.aggregateRecords(web.getRecords(), new RecordKeyValueMapper<String, MatchableTableRow, MatchableTableRow>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					MatchableTableRow record,
					DatasetIterator<Pair<String, MatchableTableRow>> resultCollector)  {
				
				// iterate over all candidate keys for the table
				for(Collection<Integer> key : tableToCandidateKeys.get(record.getTableId())) {
					
					// count all candidate key values
					HashMap<String, Integer> counts = new HashMap<>();
					
					// iterate over all candidate key attributes
					for(int columnIndex : key) {
						if(record.hasColumn(columnIndex)) {
							// count the value's occurrences
							MapUtils.increment(counts, record.get(columnIndex).toString());
						} 
					}
					
					for(String value : counts.keySet()) {
						// if a value occurred in more than one attribute of the current record's current candidate key
						if(counts.get(value)>1) {
							// it's an ambiguous value
							resultCollector.next(new Pair<String, MatchableTableRow>(value, record));
						}
					}
				}				
			}
		}, new CountAggregator<String, MatchableTableRow>());
    	
    	System.out.println("Ambiguous values");
    	final HashSet<String> ambiguous = new HashSet<>();
    	for(Pair<String, Integer> p : ambiguousValues.get()) {
    		System.out.println(String.format("\t[%d] %s", p.getSecond(), p.getFirst()));
    		ambiguous.add(p.getFirst());
    	}
    	setAmbiguousValues(ambiguous);
    	
    	// alternative to blocking via index: group all records by their values and create a lookup (effectively an inverted index)
    	// first group by all candidate key values
    	//TODO not only group by values, also group by full containment (i.e. partial key matches)?
    	// include partial candidate key matches in the process, i.e., candidate key A is a subset of candidate key B, will this result in 1:n relationship?
    	System.out.println("Grouping by candidate keys");
    	ResultSet<Group<String, MatchableTableRowWithKey>> groupedByCandidateKeyValue = proc.groupRecords(web.getRecords(), new RecordKeyValueMapper<String, MatchableTableRow, MatchableTableRowWithKey>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					MatchableTableRow record,
					DatasetIterator<Pair<String, MatchableTableRowWithKey>> resultCollector) {
				
				// iterate over all candidate keys for the table
				for(Collection<Integer> key : tableToCandidateKeys.get(record.getTableId())) {
					
					// collect all candidate key values
					ArrayList<String> values = new ArrayList<>(key.size());
					for(int columnIndex : key) {
						if(record.hasColumn(columnIndex)) {
							values.add(record.get(columnIndex).toString());
						} else {
							values.add("NULL");
						}
					}
					
					boolean isAmbiguous = false;
					//TODO currently disabled
//					for(String v : values) {
//						if(ambiguous.contains(v)) {
//							isAmbiguous = true;
//						}
//					}
					
					HashSet<String> uniqueValues = new HashSet<>(values);
					// we only use the key values for linking if they are all different
					// otherwise we would create ambiguous schema correspondences
//					if(uniqueValues.size()==values.size() && !isAmbiguous) {					
						// sort the key values (so they can be grouped regardless of their order in different tables)
						Collections.sort(values);
						
						// produce one result per candidate key
						resultCollector.next(new Pair<String, MatchableTableRowWithKey>(StringUtils.join(values, "/"),
								new MatchableTableRowWithKey(record, key)));
//					}
				}				
			}
		});
    	System.out.println(String.format("\t%d key values", groupedByCandidateKeyValue.size()));
    	
    	//TODO schema matching by grouping
    	// groupedByCandidateKeyValue contains the entity links (based on the key values)
    	// now re-group them by iterating over all groups
    	// and produce keys (key value + column value) with groups (column, row, [value index, only needed if no ambiguity avoidance is used])
    	// groups will contain one (column, row) pair for each 
    	
    	
    	// create the cross product of all groups (internally) and aggregate duplicate pairs with their count
    	// -> group by both table identifiers 
    	System.out.println("grouping pairs of rows");
    	ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs = proc.groupRecords(groupedByCandidateKeyValue, new RecordKeyValueMapper<String, Group<String, MatchableTableRowWithKey>, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Group<String, MatchableTableRowWithKey> record,
					DatasetIterator<Pair<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> resultCollector) {
				
				// get a list of all records belonging to the current group
				ArrayList<MatchableTableRowWithKey> rows = new ArrayList<>(record.getRecords().get());
				
				//TODO why can't this be a symmetric join?
				// iterate over all combinations of records (not symmetric!)
				for(int i=0;i<rows.size();i++) {
					for(int j=0;j<rows.size();j++) {
						if(i!=j) {
							MatchableTableRowWithKey record1 = rows.get(i);
							MatchableTableRowWithKey record2 = rows.get(j);
							
							// we are only interested in links between different tables (we already performed table internal de-duplication in the TableUnion step)
							if(record1.getRow().getTableId()!=record2.getRow().getTableId()) {

								if(record1.getKey().size()!=record2.getKey().size()) {
									System.out.println("whaat?");
								}
								
								// re-group by table links (from table) -> (to table)
								String groupingKey = record1.getRow().getTableId() + "/" + record2.getRow().getTableId();
								
								// create one table link per record pair as output
								resultCollector.next(
										new Pair<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>(
												groupingKey, 
												new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(record1, record2)));
							}
						}
					}
				}
			}
		});
    	
    	//TODO transform the result to a collection of BlockedMatchable
    	
    	return blockedPairs;
	}
	
}
