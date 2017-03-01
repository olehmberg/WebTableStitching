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
package de.uni_mannheim.informatik.dws.tnt.match.rules.MultiLanguageUnionTables;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumnWithFeatures;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MultiLanguageUnionTableSchemaBlocker {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTable>> runBlocking(
			DataSet<MatchableTableRow, MatchableTableColumn> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		// create a description of each attribute and match the descriptions
		
		// group by column and aggregate the values into the feature vector == transform columns into feature vectors
		
		RecordKeyValueMapper<MatchableTableColumn, MatchableTableRow, Object> groupBy = new RecordKeyValueMapper<MatchableTableColumn, MatchableTableRow, Object>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(MatchableTableRow record,
					DatasetIterator<Pair<MatchableTableColumn, Object>> resultCollector) {
				
				for(MatchableTableColumn c : record.getSchema()) {
					if(!SpecialColumns.isSpecialColumn(c)) {
						resultCollector.next(new Pair<MatchableTableColumn, Object>(c, record.get(c.getColumnIndex())));
					}
				}
				
			}
		};
		DataAggregator<MatchableTableColumn, Object, MatchableTableColumnWithFeatures> aggregator = new DataAggregator<MatchableTableColumn, Object, MatchableTableColumnWithFeatures>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public MatchableTableColumnWithFeatures aggregate(MatchableTableColumnWithFeatures previousResult, Object record) {

				
				if(previousResult.getColumn().getTableId()==23 && previousResult.getColumn().getColumnIndex()==12) {
					System.out.println("");
				}
				previousResult.updateFeatures(record);
				
				return previousResult;
			}

			@Override
			public MatchableTableColumnWithFeatures initialise(MatchableTableColumn keyValue) {
				return new MatchableTableColumnWithFeatures(keyValue);
			}
		};
		ResultSet<Pair<MatchableTableColumn, MatchableTableColumnWithFeatures>> aggregated = engine.aggregateRecords(dataset, groupBy, aggregator);
		
		RecordMapper<Pair<MatchableTableColumn, MatchableTableColumnWithFeatures>, MatchableTableColumnWithFeatures> transformation = new RecordMapper<Pair<MatchableTableColumn,MatchableTableColumnWithFeatures>, MatchableTableColumnWithFeatures>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<MatchableTableColumn, MatchableTableColumnWithFeatures> record,
					DatasetIterator<MatchableTableColumnWithFeatures> resultCollector) {
				
				MatchableTableColumn c = record.getFirst();
				MatchableTableColumnWithFeatures f = record.getSecond();
//				
//				f.setColumn(c);
//				
				resultCollector.next(f);
				
			}
		};
		ResultSet<MatchableTableColumnWithFeatures> features = engine.transform(aggregated, transformation);
		
//		// now join the feature vectors with themselves == blocking
//		Function<String, MatchableTableColumnWithFeatures> joinFeatureVectors = new Function<String, MatchableTableColumnWithFeatures>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(MatchableTableColumnWithFeatures input) {
//				return "";
//			}
//		};
//		ResultSet<Pair<MatchableTableColumnWithFeatures, MatchableTableColumnWithFeatures>> pairs = engine.symmetricSelfJoin(features, joinFeatureVectors);
		
		
		
		// join the feature vectors with themselves via the table correspondences
		
		Function<Integer, MatchableTableColumnWithFeatures> featureToTableId = new Function<Integer, MatchableTableColumnWithFeatures>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableColumnWithFeatures input) {
				return input.getColumn().getTableId();
			}
		};
		Function<Integer, Correspondence<MatchableTable, MatchableTableColumn>> correspondenceToLeftTableId = new Function<Integer, Correspondence<MatchableTable,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTable, MatchableTableColumn> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		ResultSet<Pair<MatchableTableColumnWithFeatures, Correspondence<MatchableTable, MatchableTableColumn>>> firstJoin = engine.joinMixedTypes(features, schemaCorrespondences, featureToTableId , correspondenceToLeftTableId );
		
		Function<Integer, Pair<MatchableTableColumnWithFeatures, Correspondence<MatchableTable, MatchableTableColumn>>> firstJoinToRightTableId = new Function<Integer, Pair<MatchableTableColumnWithFeatures,Correspondence<MatchableTable,MatchableTableColumn>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(
					Pair<MatchableTableColumnWithFeatures, Correspondence<MatchableTable, MatchableTableColumn>> input) {
				return input.getSecond().getSecondRecord().getTableId();
			}
		};
		ResultSet<Pair<Pair<MatchableTableColumnWithFeatures, Correspondence<MatchableTable, MatchableTableColumn>>, MatchableTableColumnWithFeatures>> secondJoin = engine.joinMixedTypes(firstJoin, features, firstJoinToRightTableId , featureToTableId);
		
		
		
		
		// calculate the similarity
		
		RecordMapper<Pair<Pair<MatchableTableColumnWithFeatures, Correspondence<MatchableTable, MatchableTableColumn>>, MatchableTableColumnWithFeatures>, Correspondence<MatchableTableColumn, MatchableTable>> match = new RecordMapper<Pair<Pair<MatchableTableColumnWithFeatures, Correspondence<MatchableTable, MatchableTableColumn>>, MatchableTableColumnWithFeatures>, Correspondence<MatchableTableColumn,MatchableTable>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Pair<MatchableTableColumnWithFeatures, Correspondence<MatchableTable, MatchableTableColumn>>, MatchableTableColumnWithFeatures> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTable>> resultCollector) {
				
				MatchableTableColumnWithFeatures f1 = record.getFirst().getFirst();
				MatchableTableColumnWithFeatures f2 = record.getSecond();
				
				boolean contentPatterns = Q.intersection(f1.getContentPatterns(), f2.getContentPatterns()).size() > 0;
				boolean tokens = Q.intersection(f1.getTokenFrequencies().keySet(), f2.getTokenFrequencies().keySet()).size()>0;
				boolean range = !(f1.getMinNumTokens()>f2.getMaxNumTokens() || f1.getMaxNumTokens() < f2.getMinNumTokens());
				boolean isEmpty = f1.isEmpty() || f2.isEmpty();
				
				if(contentPatterns || range || isEmpty) {
					
					MatchableTableColumn c1 = f1.getColumn();
					MatchableTableColumn c2 = f2.getColumn();
					
					if(c1.getTableId()>c2.getTableId()) {
						MatchableTableColumn tmp = c1;
						c1 = c2;
						c2 = tmp;
					}
					
					ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> causes = new ResultSet<>();
					causes.add(record.getFirst().getSecond());
					resultCollector.next(new Correspondence<MatchableTableColumn, MatchableTable>(c1, c2, 1.0, causes));
					System.out.println("match");
				} else {
					System.out.println("no match");
				}
				
				System.out.println(String.format("%s<->%s (patterns: %b; tokens: %b; range: %b)", f1.getColumn(), f2.getColumn(), contentPatterns, tokens, range));
				System.out.println();
				
			}
		};
		return engine.transform(secondJoin, match);
		
//		RecordMapper<Pair<MatchableTableColumnWithFeatures, MatchableTableColumnWithFeatures>, Correspondence<MatchableTableColumn, MatchableTable>> match = new RecordMapper<Pair<MatchableTableColumnWithFeatures,MatchableTableColumnWithFeatures>, Correspondence<MatchableTableColumn,MatchableTable>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Pair<MatchableTableColumnWithFeatures, MatchableTableColumnWithFeatures> record,
//					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTable>> resultCollector) {
//				
//				MatchableTableColumnWithFeatures f1 = record.getFirst();
//				MatchableTableColumnWithFeatures f2 = record.getSecond();
//				
//				boolean contentPatterns = Q.intersection(f1.getContentPatterns(), f2.getContentPatterns()).size() > 0;
//				boolean tokens = Q.intersection(f1.getTokenFrequencies().keySet(), f2.getTokenFrequencies().keySet()).size()>0;
//				boolean range = !(f1.getMinNumTokens()>f2.getMaxNumTokens() || f1.getMaxNumTokens() < f2.getMinNumTokens());
//				
//				if(contentPatterns || tokens || range) {
//					
//					MatchableTableColumn c1 = f1.getColumn();
//					MatchableTableColumn c2 = f2.getColumn();
//					
//					if(c1.getTableId()>c2.getTableId()) {
//						MatchableTableColumn tmp = c1;
//						c1 = c2;
//						c2 = tmp;
//					}
//					
//					resultCollector.next(new Correspondence<MatchableTableColumn, MatchableTable>(c1, c2, 1.0, null));
////					System.out.println("match");
//				} else {
////					System.out.println("no match");
//				}
//				
////				System.out.println(String.format("%s<->%s (patterns: %b; tokens: %b; range: %b)", f1.getColumn(), f2.getColumn(), contentPatterns, tokens, range));
////				System.out.println();
//				
//			}
//		};
//		return engine.transform(pairs, match);
	}
	
}
