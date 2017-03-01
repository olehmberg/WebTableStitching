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
package de.uni_mannheim.informatik.wdi.matching.v2;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class Aggregator<RecordTypeA, RecordTypeB> extends MatchingOperation {

	/**
	 * @param engine
	 */
	public Aggregator(DataProcessingEngine engine) {
		super(engine);
	}

	public ResultSet<Correspondence<RecordTypeA, RecordTypeB>> runAggregation(ResultSet<Correspondence<RecordTypeA, RecordTypeB>> correspondences,
			DataAggregator<Correspondence<RecordTypeA, RecordTypeB>, Correspondence<RecordTypeA, RecordTypeB>, Correspondence<RecordTypeA, RecordTypeB>> aggregate) {
		
		ResultSet<Pair<Correspondence<RecordTypeA,RecordTypeB>,Correspondence<RecordTypeA,RecordTypeB>>> aggregated = getEngine().aggregateRecords(correspondences, new CorrespondenceKeyValueMapper<RecordTypeA, RecordTypeB>(), aggregate);
		
		ResultSet<Correspondence<RecordTypeA, RecordTypeB>> result = getEngine().transform(aggregated, new PairSplitterRecordMapper<Correspondence<RecordTypeA,RecordTypeB>,Correspondence<RecordTypeA, RecordTypeB>>());
		
		return result;
		
	}
}
