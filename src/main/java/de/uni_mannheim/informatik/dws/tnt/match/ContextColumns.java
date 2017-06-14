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
import java.util.regex.Pattern;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * Utility class for creating and identifying context columns that are added in the table union step
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ContextColumns {
	
	public static final class IsContextColumnPredicate implements Func<Boolean, TableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public Boolean invoke(TableColumn in) {
			return ContextColumns.isContextColumn(in);
		}
		
	}
	
	public static final class IsNoContextColumnPredicate implements Func<Boolean, TableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public Boolean invoke(TableColumn in) {
			return !ContextColumns.isContextColumn(in);
		}
		
	}
	
	public static final class IsMatchableContextColumnPredicate implements Func<Boolean, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public Boolean invoke(MatchableTableColumn in) {
			return ContextColumns.isContextColumn(in);
		}
		
	}
	
	public static final class IsNoMatchableContextColumnPredicate implements Func<Boolean, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public Boolean invoke(MatchableTableColumn in) {
			return !ContextColumns.isContextColumn(in);
		}
		
	}
	
	public static final String PAGE_TITLE_COLUMN = "page title";
	public static final String TALBE_HEADING_COLUMN = "table heading";
	public static final Pattern URI_PART_PATTERN = Pattern.compile("^uri \\d+$");
	public static final Pattern URI_QUERY_PATTERN = Pattern.compile("^uri [\\w]+$");

	public static final Set<String> ALL = new HashSet<>(Arrays.asList(new String[] { PAGE_TITLE_COLUMN, TALBE_HEADING_COLUMN }));
	
	public static String createUriPartHeader(int uriPartIndex) {
		return String.format("uri %d", uriPartIndex);
	}
	
	public static String createUriQueryPartHeader(String parameterName) {
		return String.format("uri %s", parameterName.replaceAll("[^\\w]", ""));
	}
	
	public static boolean isContextColumn(TableColumn c) {
		return isContextColumn(c.getHeader());
	}
	
	public static boolean isContextColumn(MatchableTableColumn c) {
		return isContextColumn(c.getHeader());
	}
	
	public static boolean isContextColumn(String header) {
		return ALL.contains(header) || URI_PART_PATTERN.matcher(header).matches() || URI_QUERY_PATTERN.matcher(header).matches();
	}
	
	public static void removeContextColumns(Collection<TableColumn> columns) {
		Iterator<TableColumn> it = columns.iterator();
		while(it.hasNext()) {
			if(isContextColumn(it.next())) {
				it.remove();
			}
		}
	}
}
