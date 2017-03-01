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
package check_if_useful;

import java.util.Collection;
import java.util.LinkedList;

import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableSplitter {

	public static class ContextualTableSplit {
		public Table ContextIndependent;
		public Table ContextDependent;
	}
	
	public ContextualTableSplit splitTableByContextDependance(Table t, Collection<TableColumn> key, Collection<TableColumn> contextDependentColumns) throws Exception {
		ContextualTableSplit result = new ContextualTableSplit();
		
		LinkedList<TableColumn> ind = new LinkedList<>();
		LinkedList<TableColumn> dep = new LinkedList<>();
		
		// the first column is the original source table, which is always added
		ind.add(t.getSchema().get(0));
		dep.add(t.getSchema().get(0));
		
		// split table schema
		for(int i = 1; i < t.getColumns().size(); i++) {
			TableColumn c = t.getSchema().get(i);
			
			if(key.contains(c)) {
				// key columns are added to both tables
				ind.add(c);
				dep.add(c);
			} else if(contextDependentColumns.contains(c)) {
				// context dependent column
				dep.add(c);
			} else {
				// context independent column
				ind.add(c);
			}
		}
		
		// fill new tables with data
		result.ContextIndependent = t.project(ind);
		result.ContextDependent = t.project(dep);
		
		return result;
	}
	
}
