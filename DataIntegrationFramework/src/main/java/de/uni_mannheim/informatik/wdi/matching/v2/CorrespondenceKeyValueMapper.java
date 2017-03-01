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
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CorrespondenceKeyValueMapper<RecordTypeA, RecordTypeB> implements RecordKeyValueMapper<Correspondence<RecordTypeA, RecordTypeB>, Correspondence<RecordTypeA, RecordTypeB>, Correspondence<RecordTypeA, RecordTypeB>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper#mapRecord(java.lang.Object, de.uni_mannheim.informatik.wdi.processing.DatasetIterator)
	 */
	@Override
	public void mapRecord(Correspondence<RecordTypeA, RecordTypeB> record,
			DatasetIterator<Pair<Correspondence<RecordTypeA, RecordTypeB>, Correspondence<RecordTypeA, RecordTypeB>>> resultCollector) {
		
		Pair<Correspondence<RecordTypeA, RecordTypeB>, Correspondence<RecordTypeA, RecordTypeB>> p = new Pair<>(record, record);
		
		resultCollector.next(p);
		
	}

}
