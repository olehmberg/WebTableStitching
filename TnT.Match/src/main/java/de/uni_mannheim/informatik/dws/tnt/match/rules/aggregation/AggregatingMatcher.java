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
public class AggregatingMatcher<InType1, InType2, OutType1, OutType2, GroupingType> {

	private RecordKeyValueMapper<GroupingType, Correspondence<InType1, InType2>, Correspondence<InType1, InType2>> groupByMapper;
	private DataAggregator<GroupingType, Correspondence<InType1, InType2>, Correspondence<OutType1, OutType2>> aggregator;
	
	public AggregatingMatcher(RecordKeyValueMapper<GroupingType, Correspondence<InType1, InType2>, Correspondence<InType1, InType2>> groupByMapper, DataAggregator<GroupingType, Correspondence<InType1, InType2>, Correspondence<OutType1, OutType2>> aggregator) {
		this.groupByMapper = groupByMapper;
		this.aggregator = aggregator;
	}
	
	public ResultSet<Correspondence<OutType1, OutType2>> aggregate(ResultSet<Correspondence<InType1, InType2>> correspondences,
			DataProcessingEngine proc) {
		
		ResultSet<Pair<GroupingType, Correspondence<OutType1, OutType2>>> aggregated = proc.aggregateRecords(correspondences, groupByMapper, aggregator);
		
		return proc.transform(aggregated, new RecordMapper<Pair<GroupingType, Correspondence<OutType1, OutType2>>, Correspondence<OutType1, OutType2>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<GroupingType, Correspondence<OutType1, OutType2>> record,
					DatasetIterator<Correspondence<OutType1, OutType2>> resultCollector) {
			
				resultCollector.next(record.getSecond());
				
			}
		});
		
	}
	
	
}
