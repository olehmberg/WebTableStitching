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
package de.uni_mannheim.informatik.wdi.matching.blocking.recordmappers;

import de.uni_mannheim.informatik.wdi.matching.MatchingTask;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class JoinedGroupsToMatchingTaskMapper<KeyType, RecordType extends Matchable, SchemaElementType> implements RecordMapper<Pair<Group<KeyType,RecordType>,Group<KeyType,RecordType>>, BlockedMatchable<RecordType, SchemaElementType>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences;
	
	/**
	 * 
	 */
	public JoinedGroupsToMatchingTaskMapper(ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		this.schemaCorrespondences = schemaCorrespondences;
	}
	
	@Override
	public void mapRecord(
			Pair<Group<KeyType, RecordType>, Group<KeyType, RecordType>> record,
			DatasetIterator<BlockedMatchable<RecordType, SchemaElementType>> resultCollector) {
		for(RecordType web : record.getFirst().getRecords().get()){
			for(RecordType kb : record.getSecond().getRecords().get()){
				resultCollector.next(new MatchingTask<RecordType, SchemaElementType>(web, kb, schemaCorrespondences));	
			}
		}
	}
}
