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
package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowWithKey {
	
	public static class RowNumberComparator implements Comparator<MatchableTableRowWithKey>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(MatchableTableRowWithKey o1, MatchableTableRowWithKey o2) {
			return Integer.compare(o1.getRow().getRowNumber(), o2.getRow().getRowNumber());
		}
		
	}
	
	private MatchableTableRow row;
	private Collection<Integer> key;
	public MatchableTableRow getRow() {
		return row;
	}
	public Collection<Integer> getKey() {
		return key;
	}
	/**
	 * @param row
	 * @param key
	 */
	public MatchableTableRowWithKey(MatchableTableRow row,
			Collection<Integer> key) {
		super();
		this.row = row;
		this.key = key;
	}
}
