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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ValueBasedSchemaBlocker {

	private boolean noFiltering = false;
	private double matchThreshold = 0.0;
	
	public ValueBasedSchemaBlocker() {
	}

	public ValueBasedSchemaBlocker(boolean noFiltering,double matchThreshold) {
		this.noFiltering = noFiltering;
		this.matchThreshold = matchThreshold;
	}
	
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> runBlocking(
			BasicCollection<MatchableTableRow> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		// group by all values and aggregate the count of attribute pairs from different tables
		
		RecordKeyValueMapper<String, MatchableTableRow, MatchableTableColumn> groupBy = new RecordKeyValueMapper<String, MatchableTableRow, MatchableTableColumn>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(MatchableTableRow record, DatasetIterator<Pair<String, MatchableTableColumn>> resultCollector) {
				
				for(MatchableTableColumn col : record.getSchema()) {
					if(!SpecialColumns.isSpecialColumn(col)) {
						Object value = record.get(col.getColumnIndex());
						
						if(value!=null) {
							
							//TODO add table-level blocking key to the value
							
							resultCollector.next(new Pair<String, MatchableTableColumn>(value.toString(), col));
						}
					}
				}
				
			}
		};
		
		// set means that each DISTINCT value will result in a vote, so 100 matches with the same value will count as 1
		DataAggregator<String, MatchableTableColumn, Collection<MatchableTableColumn>> setAggregator = new DataAggregator<String,MatchableTableColumn, Collection<MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Collection<MatchableTableColumn> aggregate(Collection<MatchableTableColumn> previousResult,
					MatchableTableColumn record) {
				
				if(previousResult==null) {
					previousResult = new HashSet<>();
				}
				
				previousResult.add(record);
				
				return previousResult;
			}

			@Override
			public Set<MatchableTableColumn> initialise(String keyValue) {
				return null;
			}
		};
		//TODO list is too expensive? aggregate counts directly ...
		DataAggregator<String, MatchableTableColumn, Collection<MatchableTableColumn>> listAggregator = new DataAggregator<String,MatchableTableColumn, Collection<MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Collection<MatchableTableColumn> aggregate(Collection<MatchableTableColumn> previousResult,
					MatchableTableColumn record) {
				
				if(previousResult==null) {
					previousResult = new LinkedList<>();
				}
				
				previousResult.add(record);
				
				return previousResult;
			}

			@Override
			public Set<MatchableTableColumn> initialise(String keyValue) {
				return null;
			}
		};
		ResultSet<Pair<String, Collection<MatchableTableColumn>>> valueToAttributeIndex = engine.aggregateRecords(dataset, groupBy, setAggregator);
		
		// now create pairs from the groups and aggregate per table combination
		RecordKeyValueMapper<List<Integer>, Pair<String, Collection<MatchableTableColumn>>, Pair<MatchableTableColumn, MatchableTableColumn>> groupByTableCombination = new RecordKeyValueMapper<List<Integer>, Pair<String,Collection<MatchableTableColumn>>, Pair<MatchableTableColumn,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<String, Collection<MatchableTableColumn>> record,
					DatasetIterator<Pair<List<Integer>, Pair<MatchableTableColumn, MatchableTableColumn>>> resultCollector) {
				
				List<MatchableTableColumn> list = new ArrayList<>(record.getSecond());
				
				list = Q.sort(list, new MatchableTableColumn.TableIdColumnIndexComparator());
				
				for(int i = 0; i < list.size(); i++) {
					MatchableTableColumn c1 = list.get(i);
					for(int j = i+1; j < list.size(); j++) {
						MatchableTableColumn c2 = list.get(j);
						
						if(c1.getTableId()!=c2.getTableId()) {
							resultCollector.next(new Pair<List<Integer>, Pair<MatchableTableColumn,MatchableTableColumn>>(Q.toList(c1.getTableId(), c2.getTableId()), new Pair<>(c1, c2)));
						}
					}
				}
			}
		};
		DataAggregator<List<Integer>, Pair<MatchableTableColumn, MatchableTableColumn>, SimilarityMatrix<MatchableTableColumn>> matrixAggregator = new DataAggregator<List<Integer>, Pair<MatchableTableColumn,MatchableTableColumn>, SimilarityMatrix<MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public SimilarityMatrix<MatchableTableColumn> aggregate(SimilarityMatrix<MatchableTableColumn> previousResult,
					Pair<MatchableTableColumn, MatchableTableColumn> record) {

				if(previousResult==null) {
					previousResult=new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
				}
				
				previousResult.add(record.getFirst(), record.getSecond(), 1.0);
				
				return previousResult;
			}

			@Override
			public SimilarityMatrix<MatchableTableColumn> initialise(List<Integer> keyValue) {
				return null;
			}
		};
		ResultSet<Pair<List<Integer>, SimilarityMatrix<MatchableTableColumn>>> votesPerTablePair = engine.aggregateRecords(valueToAttributeIndex, groupByTableCombination, matrixAggregator);
		
		// create correspondences
		RecordMapper<Pair<List<Integer>, SimilarityMatrix<MatchableTableColumn>>, Correspondence<MatchableTableColumn, MatchableTableRow>> matrixToCorrespondences = new RecordMapper<Pair<List<Integer>,SimilarityMatrix<MatchableTableColumn>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<List<Integer>, SimilarityMatrix<MatchableTableColumn>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {

				int t1 = record.getFirst().get(0);
				int t2 = record.getFirst().get(1);
				
				if(t1>t2) {
					System.out.println("Wrong Direction!");
				}
				
				SimilarityMatrix<MatchableTableColumn> m = record.getSecond();
				
				
//				System.out.println(String.format("Attribute matches for #%d <-> #%d", t1, t2));
//				System.out.println(m.getOutput());
				
				if(!noFiltering) {
					BestChoiceMatching bcm = new BestChoiceMatching();
					m = bcm.match(m);
				}

				for(MatchableTableColumn c1 : m.getFirstDimension()) {
					for(MatchableTableColumn c2 : m.getMatches(c1)) {
						
//						if(m.get(c1, c2)>0.0) {
						if(m.get(c1, c2)>matchThreshold) {
							Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, m.get(c1, c2), null);
							
							resultCollector.next(cor);
						}
						
					}
				}
			}
		};
		return engine.transform(votesPerTablePair, matrixToCorrespondences);
	}
	
}
