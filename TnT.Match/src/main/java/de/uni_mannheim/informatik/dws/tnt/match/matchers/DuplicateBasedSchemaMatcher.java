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
package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import java.util.HashMap;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.DuplicateBasedWebTableSchemaMatchingRule;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DuplicateBasedSchemaMatcher {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> match(Table t1, Table t2, WebTables web, MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, double minSimilarity) {
    	/***********************************************
    	 * Duplicate-Based Schema Matching
    	 ***********************************************/
		
		DefaultDataSet<MatchableTableColumn, MatchableTableColumn> schema1 = new DefaultDataSet<>();
		DefaultDataSet<MatchableTableColumn, MatchableTableColumn> schema2 = new DefaultDataSet<>();
		Map<Integer, Map<Integer, String>> tableColumnIdentifiers = new HashMap<>();
		
		// prepare schema for t1
		Map<Integer, String> tColumnMap = new HashMap<>();
		int t1Id = web.getTableIndices().get(t1.getPath());
		tableColumnIdentifiers.put(web.getTableIndices().get(t1.getPath()), tColumnMap);
		
		//TODO if a column has an FD with an empty determinant, then it should not be allowed to vote against a correspondence?
		
		// create the schema for table t1
		for(TableColumn c : t1.getColumns()) {
			if(!SpecialColumns.ALL.contains(c.getHeader())) {
				MatchableTableColumn col = new MatchableTableColumn(t1Id, c);
				schema1.add(col);
				
				// map the column index to the column ID
				tColumnMap.put(c.getColumnIndex(), c.getIdentifier());
			}
		}
		
		// prepare schema for t2
		Map<Integer, String> centroidColumnMap = new HashMap<>();
		int t2Id = web.getTableIndices().get(t2.getPath());
		tableColumnIdentifiers.put(t2Id, centroidColumnMap);
		
		// create the schema for table t2
		for(TableColumn c : t2.getColumns()) {
			if(!SpecialColumns.ALL.contains(c.getHeader())) {
				MatchableTableColumn col = new MatchableTableColumn(t2Id, c);
				schema2.add(col);
				centroidColumnMap.put(c.getColumnIndex(), c.getIdentifier());
			}
		}
		
		// create the schema matching rule
		DuplicateBasedWebTableSchemaMatchingRule rule = new DuplicateBasedWebTableSchemaMatchingRule(minSimilarity);
		rule.setTableColumnIdentifiers(tableColumnIdentifiers);
//		rule.setUseLocalAmbiguityAvoidance(true);
		rule.setUseLocalAmbiguityAvoidance(false);
		
		
		//TODO figure out what the column identifiers are used for and how to replace them when matching all columns at once
		// -- maps column index to column id
		// -- is the connection between the values of a MatchableTableRow and the corresponding MatchableTableColumn
		// -- -> create that mapping only using table id and column index, which should be enough information and is already available
		
		// run the schema matching
		return matchingEngine.runDuplicateBasedSchemaMatching(schema1, schema2, instanceCorrespondences, rule);
	}
	
}
