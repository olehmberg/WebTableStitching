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

import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTable implements Matchable {

	private int tableId;
	private int tableIndex;
	private MatchableTableColumn[] schema;
	private MatchableTableColumn[][] keys;//TODO replace by indices in schema
	
	public MatchableTable(Table t, MatchableTableColumn[] schema) {
		this.tableId = t.getTableId();
		this.tableIndex = t.getContext().getTableNum();
		this.schema = schema;
	}
	
	public MatchableTable() {
	}
	
	public int getTableId() {
		return tableId;
	}
	public void setTableId(int tableId) {
		this.tableId = tableId;
	}
	public MatchableTableColumn[] getSchema() {
		return schema;
	}
	public void setSchema(MatchableTableColumn[] schema) {
		this.schema = schema;
	}
	public MatchableTableColumn[][] getKeys() {
		return keys;
	}
	public void setKeys(MatchableTableColumn[][] keys) {
		this.keys = keys;
	}
	
	public int getTableIndex() {
		return tableIndex;
	}

	public void setTableIndex(int tableIndex) {
		this.tableIndex = tableIndex;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getIdentifier()
	 */
	@Override
	public String getIdentifier() {
		return Integer.toString(tableId);
	}
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getProvenance()
	 */
	@Override
	public String getProvenance() {
		return null;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("{#%d}", getTableId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + tableId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MatchableTable other = (MatchableTable) obj;
		if (tableId != other.tableId)
			return false;
		return true;
	}
	
	
}
