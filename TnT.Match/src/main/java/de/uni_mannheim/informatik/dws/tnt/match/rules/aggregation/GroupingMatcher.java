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
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public abstract class GroupingMatcher<InType1, InType2, OutType1, OutType2, GroupingType> {

	private RecordKeyValueMapper<GroupingType, Correspondence<InType1, InType2>, Correspondence<InType1, InType2>> groupByMapper;
	private RecordMapper<Group<GroupingType, Correspondence<InType1, InType2>>, Correspondence<OutType1, OutType2>> transformationMapper;
	
	public GroupingMatcher(RecordKeyValueMapper<GroupingType, Correspondence<InType1, InType2>, Correspondence<InType1, InType2>> groupByMapper, RecordMapper<Group<GroupingType, Correspondence<InType1, InType2>>, Correspondence<OutType1, OutType2>> transformationMapper) {
		this.groupByMapper = groupByMapper;
		this.transformationMapper = transformationMapper;
	}
	
	public ResultSet<Correspondence<OutType1, OutType2>> aggregate(ResultSet<Correspondence<InType1, InType2>> correspondences,
			DataProcessingEngine proc) {
		
		// group records
		ResultSet<Group<GroupingType, Correspondence<InType1, InType2>>> grouped = proc.groupRecords(correspondences, groupByMapper);
		
		// transform the groups to the output correspondences
		return proc.transform(grouped, transformationMapper);
		
	}
	
}
