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
package de.uni_mannheim.informatik.dws.t2k.match;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ClassRefinement {

	private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
	private Map<Integer, Map<Integer, Integer>> propertyIndices;
//	private Map<Integer, String> classIndices;
	private Map<String, String> classHierarchy;
	private ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences;
	private Map<Integer, Set<String>> classesPerTable;
	private Map<String, Integer> classIds;
	
	private Map<Integer, String> finalClassPerTable;
	/**
	 * @return the finalClassPerTable
	 */
	public Map<Integer, String> getFinalClassPerTable() {
		return finalClassPerTable;
	}
	
	public ClassRefinement(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, Map<Integer, Map<Integer, Integer>> propertyIndices, Map<String, String> classHierarchy,ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, Map<Integer, Set<String>> classesPerTable, Map<String, Integer> classIds) {
		//Map<Integer, String> classIndices,
		this.matchingEngine = matchingEngine;
		this.propertyIndices = propertyIndices;
//		this.classIndices = classIndices;
		this.classHierarchy = classHierarchy;
		this.schemaCorrespondences= schemaCorrespondences;
		this.classesPerTable = classesPerTable;
		this.classIds = classIds;
	}
	
	public Map<Integer, Set<String>> run() {
		DataProcessingEngine proc = matchingEngine.getProcessingEngine();
		
		// sum up the property correspondence scores per class
		RecordKeyValueMapper<Pair<Integer, String>, Correspondence<MatchableTableColumn, MatchableTableRow>, Double> groupByClassProperty = new RecordKeyValueMapper<Pair<Integer, String>, Correspondence<MatchableTableColumn,MatchableTableRow>, Double>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableColumn, MatchableTableRow> record,
					DatasetIterator<Pair<Pair<Integer, String>, Double>> resultCollector) {
				
				// group the similarity values per class/property combination
				// a property will be counted for multiple classes!
				
				int propertyId = record.getSecondRecord().getColumnIndex();
				
				Set<String> classes = classesPerTable.get(record.getFirstRecord().getTableId());
				
				if(classes!=null) {
					// go through all classes from the previous class matching step
					for(String className : classes) {
						
						int classId = classIds.get(className);
						Map<Integer, Integer> properties = propertyIndices.get(classId);
						
						// check if the class has the property
						if(properties.containsKey(propertyId)) {
							
							// if yes, add the similarity value
	
							// the web table id + the class name is the grouping key
							Pair<Integer, String> groupingKey = new Pair<Integer, String>(record.getFirstRecord().getTableId(), className);
							
							resultCollector.next(new Pair<Pair<Integer, String>, Double>(groupingKey, record.getSimilarityScore()));
						}
						
					}
				}
			}
		};
		
		DataAggregator<Pair<Integer, String>, Double, Double> sumSimilarityScore = new DataAggregator<Pair<Integer, String>, Double, Double>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			public Double initialise(Pair<Integer, String> keyValue) {
				return 0.0;
			}
			
			@Override
			public Double aggregate(Double previousResult, Double record) {
				return previousResult+record;
			}
		};
		ResultSet<Pair<Pair<Integer, String>, Double>> classScores = proc.aggregateRecords(schemaCorrespondences, groupByClassProperty, sumSimilarityScore);
		
		// now we have all class scores for all tables
		// and we can pick the class with the highest score for each table
		
		RecordKeyValueMapper<Integer, Pair<Pair<Integer, String>, Double>, Pair<String, Double>> groupByTableId = new RecordKeyValueMapper<Integer, Pair<Pair<Integer,String>,Double>, Pair<String,Double>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Pair<Integer, String>, Double> record,
					DatasetIterator<Pair<Integer, Pair<String, Double>>> resultCollector) {
				
				// the grouping key is the web table id
				int groupingKey = record.getFirst().getFirst();
				
				// the value is the class with its similarity score
				Pair<String, Double> value = new Pair<String, Double>(record.getFirst().getSecond(), record.getSecond());
				
				resultCollector.next(new Pair<Integer, Pair<String,Double>>(groupingKey, value));
				
			}
		};
		DataAggregator<Integer, Pair<String, Double>, Pair<String, Double>> selectMaxClass = new DataAggregator<Integer, Pair<String,Double>, Pair<String,Double>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Pair<String, Double> initialise(Integer keyValue) {
				return null;
			}
			
			@Override
			public Pair<String, Double> aggregate(Pair<String, Double> previousResult, Pair<String, Double> record) {
				if(previousResult==null) {
					return record;
				} else {
					if(record.getSecond()>previousResult.getSecond()) {
						return record;
					} else {
						return previousResult;
					}
				}
			}
		};
		ResultSet<Pair<Integer, Pair<String, Double>>> bestClassPerTable = proc.aggregateRecords(classScores, groupByTableId, selectMaxClass);
		
		// we have selected the best class for each table, now bring it into the correct data format
		HashMap<Integer, Set<String>> classesPerTable = new HashMap<>();
		finalClassPerTable = new HashMap<>();
		
		for(Pair<Integer, Pair<String, Double>> classMapping : bestClassPerTable.get()) {
			Set<String> classes = Q.toSet(classMapping.getSecond().getFirst());
			
			String superClass = classHierarchy.get(classMapping.getSecond().getFirst());
			while(superClass!=null) {
				classes.add(superClass);
				superClass = classHierarchy.get(superClass);
			}
			
			classesPerTable.put(classMapping.getFirst(), classes);
			finalClassPerTable.put(classMapping.getFirst(), classMapping.getSecond().getFirst());
		}
		
		return classesPerTable;
	}
	
}
