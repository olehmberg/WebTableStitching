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

import java.util.List;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
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
public class ColumnIndexAggregator {


	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> aggregate(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences,
			DataProcessingEngine proc) {
		
		// count number of correspondences between column indices
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> sorted = proc.sort(correspondences, new Function<String, Correspondence<MatchableTableColumn, MatchableTableRow>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(Correspondence<MatchableTableColumn, MatchableTableRow> in) {
				return String.format("#%d/#%d-[%d]/[%d]", in.getFirstRecord().getTableId(), in.getSecondRecord().getTableId(), in.getFirstRecord().getColumnIndex(), in.getSecondRecord().getColumnIndex());
			}
		});
		
		RecordKeyValueMapper<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>> groupByColummnIndexCombination = new RecordKeyValueMapper<List<Integer>, Correspondence<MatchableTableColumn,MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableColumn, MatchableTableRow> record,
					DatasetIterator<Pair<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {

//				System.out.println(String.format("%d\t%d\t%d\t%d\t%s\t%s", record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId(), record.getFirstRecord().getColumnIndex(), record.getSecondRecord().getColumnIndex(), record.getFirstRecord().getHeader(), record.getSecondRecord().getHeader()));
				
				resultCollector.next(new Pair<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>(Q.sort(Q.toList(record.getFirstRecord().getColumnIndex(), record.getSecondRecord().getColumnIndex())), record));
				
			}
		};
		
		DataAggregator<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>> correspondenceAggregator = new DataAggregator<List<Integer>, Correspondence<MatchableTableColumn,MatchableTableRow>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Correspondence<MatchableTableColumn, MatchableTableRow> aggregate(
					Correspondence<MatchableTableColumn, MatchableTableRow> previousResult,
					Correspondence<MatchableTableColumn, MatchableTableRow> record) {
				
				if(previousResult==null) {
					MatchableTableColumn c1 = null, c2 = null;
					
					c1 = record.getFirstRecord();
					c2 = record.getSecondRecord();
//					
//					if(c1.getColumnIndex()>c2.getColumnIndex()) {
//						MatchableTableColumn tmp = c1;
//						c1 = c2;
//						c2 = tmp;
//					}
//					
					
					previousResult = new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, 1, null);
				} else {
					double sim = previousResult.getSimilarityScore() + 1;
					previousResult.setsimilarityScore(sim);
				}
				
				return previousResult;
			}

			@Override
			public Correspondence<MatchableTableColumn, MatchableTableRow> initialise(List<Integer> keyValue) {
				return null;
			}
		};
		ResultSet<Pair<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>> aggregated = proc.aggregateRecords(sorted, groupByColummnIndexCombination, correspondenceAggregator);
		
		return proc.transform(aggregated, new RecordMapper<Pair<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn, MatchableTableRow>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
				
				resultCollector.next(record.getSecond());
				
			}
		});
		
	}
	
}
