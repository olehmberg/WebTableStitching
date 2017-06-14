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
package de.uni_mannheim.informatik.dws.tnt.match;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SpecialColumns {

	public static final String SOURCE_TABLE_COLUMN = "source table";
	public static final String ROW_NUMBER_COLUMN = "row number";

	public static final Set<String> ALL = new HashSet<>(Arrays.asList(new String[] { SOURCE_TABLE_COLUMN, ROW_NUMBER_COLUMN }));
	
	public static boolean isSpecialColumn(TableColumn c) {
		return ALL.contains(c.getHeader());
	}
	
	public static boolean isSpecialColumn(MatchableTableColumn c) {
		return ALL.contains(c.getHeader());
	}
	
	public static void removeSpecialColumns(Collection<TableColumn> columns) {
		Iterator<TableColumn> it = columns.iterator();
		while(it.hasNext()) {
			if(isSpecialColumn(it.next())) {
				it.remove();
			}
		}
	}
}
