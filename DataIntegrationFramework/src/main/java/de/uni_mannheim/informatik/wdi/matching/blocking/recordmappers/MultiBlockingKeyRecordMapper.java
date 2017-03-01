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

import java.util.Collection;

import de.uni_mannheim.informatik.wdi.matching.blocking.MultiBlockingKeyGenerator;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MultiBlockingKeyRecordMapper<RecordType extends Matchable> implements RecordKeyValueMapper<String, RecordType, RecordType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private MultiBlockingKeyGenerator<RecordType> blockingFunction;
	
	/**
	 * 
	 */
	public MultiBlockingKeyRecordMapper(MultiBlockingKeyGenerator<RecordType> blockingFunction) {
		this.blockingFunction = blockingFunction;
	}
	
	@Override
	public void mapRecord(RecordType record,
			DatasetIterator<Pair<String, RecordType>> resultCollector) {
		
		Collection<String> blockingKeys = blockingFunction.getMultiBlockingKey(record);
		
		for(String blockingKey : blockingKeys) {
			resultCollector.next(new Pair<String, RecordType>(blockingKey, record));
		}
	}
}