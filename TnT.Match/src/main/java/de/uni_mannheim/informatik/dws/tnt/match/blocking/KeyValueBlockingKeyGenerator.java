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
package de.uni_mannheim.informatik.dws.tnt.match.blocking;

import java.util.Collection;
import java.util.LinkedList;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.blocking.MultiBlockingKeyGenerator;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KeyValueBlockingKeyGenerator extends MultiBlockingKeyGenerator<MatchableTableRow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.MultiBlockingKeyGenerator#getMultiBlockingKey(de.uni_mannheim.informatik.wdi.model.Matchable)
	 */
	@Override
	public Collection<String> getMultiBlockingKey(MatchableTableRow instance) {

		Collection<String> blockingKeys = new LinkedList<>();
		
		for(MatchableTableColumn[] key : instance.getKeys()) {
			blockingKeys.add(StringUtils.join(Q.toList(instance.get(Q.toPrimitiveIntArray(Q.project(Q.toList(key), new MatchableTableColumn.ColumnIndexProjection())))), "/"));
		}
		
		return blockingKeys;
	}

}
