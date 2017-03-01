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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableKey;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableKeyBlockingKeyGenerator extends KeyBasedBlockingKeyGenerator<MatchableTableRow, WebTableKey> {


	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.blocking.KeyBasedBlockingKeyGenerator#getBlockingKey(de.uni_mannheim.informatik.wdi.model.Matchable, java.lang.Object)
	 */
	@Override
	public String getBlockingKey(MatchableTableRow record, WebTableKey key) {
		// collect all candidate key values
		// we have to use String here as Objects cannot be sorted
		ArrayList<String> values = new ArrayList<>(key.getColumnIndices().size());
		for(int columnIndex : key.getColumnIndices()) {
			if(record.hasColumn(columnIndex)) {
				values.add(record.get(columnIndex).toString());
			} else {
				values.add("NULL");
			}
		}
		
		// we only use the key values for linking if they are all different
		// otherwise we would create ambiguous schema correspondences
		switch (getAmbiguityAvoidanceMode()) {
		case Local:
			
			// create a set of all values
			Set<String> uniqueValues = new HashSet<>(values);
			
			// if a value appears multiple time, don't use the record
			if(uniqueValues.size()!=values.size()) {
				return null;
			}

			break;

		case Global:
			for(Object v : values) {
				// if one of the values is in the set of ambiguous values
				if(getAmbiguousValues().contains(v)) {
					// don't use the record
					return null;
				}
			}
			break;			
		default:
			break;
		}

		// sort the key values (so they can be grouped regardless of their order in different tables)
		Collections.sort(values);
		
		// create the blocking key
		return StringUtils.join(values, "/");
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.blocking.KeyBasedBlockingKeyGenerator#getKeyValues(de.uni_mannheim.informatik.wdi.model.Matchable, java.lang.Object)
	 */
	@Override
	public Object[] getKeyValues(MatchableTableRow record, WebTableKey key) {
		ArrayList<String> values = new ArrayList<>(key.getColumnIndices().size());
		for(int columnIndex : key.getColumnIndices()) {
			if(record.hasColumn(columnIndex)) {
				values.add(record.get(columnIndex).toString());
			} else {
				values.add("NULL");
			}
		}
		return values.toArray();
	}

}
