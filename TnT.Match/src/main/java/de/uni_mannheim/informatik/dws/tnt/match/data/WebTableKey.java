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

import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.wdi.processing.Function;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableKey implements Comparable<WebTableKey> {
	
	public static class WebTableKeyToTableId implements Function<Integer, WebTableKey> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Integer execute(WebTableKey input) {
			return input.getTableId();
		}
	}
	
	private int tableId;
	private Set<Integer> columnIndices;
	
	/**
	 * @return the tableId
	 */
	public int getTableId() {
		return tableId;
	}
	
	/**
	 * @return the columnIndices
	 */
	public Set<Integer> getColumnIndices() {
		return columnIndices;
	}
	
	public WebTableKey(int tableId, Set<Integer> columnIndices) {
		this.tableId = tableId;
		this.columnIndices = columnIndices;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnIndices == null) ? 0 : columnIndices.hashCode());
		result = prime * result + tableId;
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof WebTableKey) {
			WebTableKey k = (WebTableKey)obj;
			return tableId==k.tableId && columnIndices.equals(k.columnIndices);
		}
		return super.equals(obj);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(WebTableKey o) {
		int result = Integer.compare(getTableId(), o.getTableId());
		
		if(result==0) {
			result = Integer.compare(getColumnIndices().size(), o.getColumnIndices().size());
		}
		
		if(result==0) {
			List<Integer> l1 = Q.sort(getColumnIndices());
			List<Integer> l2 = Q.sort(o.getColumnIndices());
			
			for(int i=0; i<l1.size() && result==0; i++) {
				result = Integer.compare(l1.get(i), l2.get(i));
			}
		}
	
		return result;
	}

}
