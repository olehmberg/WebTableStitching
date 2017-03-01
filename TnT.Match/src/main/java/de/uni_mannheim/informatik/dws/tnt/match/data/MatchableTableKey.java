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

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.processing.Function;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableKey implements Matchable, Comparable<MatchableTableKey> {
	
	public static class WebTableKeyToTableId implements Function<Integer, MatchableTableKey> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Integer execute(MatchableTableKey input) {
			return input.getTableId();
		}
	}
	
	public static class TableIdProjection implements Func<Integer, MatchableTableKey> {

		@Override
		public Integer invoke(MatchableTableKey input) {
			return input.getTableId();
		}
	}
	
	public static class ColumnsProjection implements Func<Set<MatchableTableColumn>, MatchableTableKey> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.wdi.processing.Function#execute(java.lang.Object)
		 */
		@Override
		public Set<MatchableTableColumn> invoke(MatchableTableKey input) {
			return input.getColumns();
		}
		
	}
	
	private int tableId;
	private Set<MatchableTableColumn> columns;
	
	/**
	 * @return the tableId
	 */
	public int getTableId() {
		return tableId;
	}
	
	/**
	 * @return the columnIndices
	 */
	public Set<MatchableTableColumn> getColumns() {
		return columns;
	}
	
	public MatchableTableKey(int tableId, Set<MatchableTableColumn> columns) {
		this.tableId = tableId;
		this.columns = columns;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columns == null) ? 0 : columns.hashCode());
		result = prime * result + tableId;
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof MatchableTableKey) {
			MatchableTableKey k = (MatchableTableKey)obj;
			return tableId==k.tableId && columns.equals(k.columns);
		}
		return super.equals(obj);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(MatchableTableKey o) {
		int result = Integer.compare(getTableId(), o.getTableId());
		
		if(result==0) {
			result = Integer.compare(getColumns().size(), o.getColumns().size());
		}
		
		if(result==0) {
			List<MatchableTableColumn> l1 = Q.sort(getColumns(), new MatchableTableColumn.ColumnIndexComparator());
			List<MatchableTableColumn> l2 = Q.sort(o.getColumns(), new MatchableTableColumn.ColumnIndexComparator());
			
			for(int i=0; i<l1.size() && result==0; i++) {
				result = Integer.compare(l1.get(i).getColumnIndex(), l2.get(i).getColumnIndex());
			}
		}
	
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
//		return String.format("{%s}", StringUtils.join(Q.project(columns, new MatchableTableColumn.ColumnHeaderProjection()), ","));
		return String.format("{%s}", columns);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getIdentifier()
	 */
	@Override
	public String getIdentifier() {
		return String.format("{#%d}%s", tableId, columns);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getProvenance()
	 */
	@Override
	public String getProvenance() {
		return null;
	}
}
