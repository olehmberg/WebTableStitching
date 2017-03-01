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

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.DateTime;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class Matcher<RecordTypeA, RecordTypeB> extends MatchingOperation {

	private MatchingRule<RecordTypeA, RecordTypeB> matchingRule;
	
	public Matcher(DataProcessingEngine engine, MatchingRule<RecordTypeA, RecordTypeB> matchingRule) {
		super(engine);
		this.matchingRule = matchingRule;
	}
	
	public ResultSet<Correspondence<RecordTypeA, RecordTypeB>> runMatching(ResultSet<Correspondence<RecordTypeA, RecordTypeB>> typeACorrespondences) {
		
		long start = System.currentTimeMillis();
		
		MatcherRecordMapper<RecordTypeA, RecordTypeB> recordMapper = new MatcherRecordMapper<>(matchingRule);
		
		ResultSet<Correspondence<RecordTypeA, RecordTypeB>> result = getEngine().transform(typeACorrespondences, recordMapper);

		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Matcher finished after %s; found %,d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), result.size()));

		return result;
	}
	
}
