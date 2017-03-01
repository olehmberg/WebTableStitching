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

import java.util.List;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.AggregatingMatcher;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MultiLanguageUnionSchemaToTableAggregator extends AggregatingMatcher<MatchableTableColumn, MatchableTable, MatchableTable, MatchableTableColumn, List<MatchableTable>> {

	
	private static RecordKeyValueMapper<List<MatchableTable>, Correspondence<MatchableTableColumn, MatchableTable>, Correspondence<MatchableTableColumn, MatchableTable>> groupByMapper = new RecordKeyValueMapper<List<MatchableTable>, Correspondence<MatchableTableColumn,MatchableTable>, Correspondence<MatchableTableColumn,MatchableTable>>() {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void mapRecord(Correspondence<MatchableTableColumn, MatchableTable> record,
				DatasetIterator<Pair<List<MatchableTable>, Correspondence<MatchableTableColumn, MatchableTable>>> resultCollector) {
			
			Correspondence<MatchableTable, MatchableTableColumn> tableCor = Q.firstOrDefault(record.getCausalCorrespondences().get());
			
			// group by table combination
			resultCollector.next(new Pair<List<MatchableTable>, Correspondence<MatchableTableColumn,MatchableTable>>(Q.toList(tableCor.getFirstRecord(), tableCor.getSecondRecord()), record));
			
			
		}
	};
	
	private static DataAggregator<List<MatchableTable>, Correspondence<MatchableTableColumn, MatchableTable>, Correspondence<MatchableTable, MatchableTableColumn>> aggregator = new DataAggregator<List<MatchableTable>, Correspondence<MatchableTableColumn,MatchableTable>, Correspondence<MatchableTable,MatchableTableColumn>>() {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Correspondence<MatchableTable, MatchableTableColumn> aggregate(
				Correspondence<MatchableTable, MatchableTableColumn> previousResult,
				Correspondence<MatchableTableColumn, MatchableTable> record) {

			previousResult.setsimilarityScore(previousResult.getSimilarityScore() + 1);
			
			return previousResult;
		}
		
		public Correspondence<MatchableTable,MatchableTableColumn> initialise(List<MatchableTable> keyValue) {
			return new Correspondence<MatchableTable, MatchableTableColumn>(keyValue.get(0), keyValue.get(1), 0.0, null);
		};
	};
	
	/**
	 * @param groupByMapper
	 * @param transformationMapper
	 */
	public MultiLanguageUnionSchemaToTableAggregator() {
		super(groupByMapper, aggregator);
	}



	
}
