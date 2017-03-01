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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.rules.CandidateKeyConsolidator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyByMultiSchemaCorrespondenceBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyBySchemaCorrespondenceBlocker;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DeterminantMatcher {

	private boolean noEarlyFiltering = false;
	
	public DeterminantMatcher() {
	}
	
	public DeterminantMatcher(boolean noEarlyFiltering) {
		this.noEarlyFiltering = noEarlyFiltering;
	}
	
	public ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> match(
			WebTables web,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, 
			DataProcessingEngine proc) {
		
    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = null;
    	
    	// initialise the matching keys with the FDs that were discovered on the union tables
//    	ResultSet<MatchableTableKey> consolidatedKeys = createFDs(web,2);
    	ResultSet<MatchableTableKey> consolidatedKeys = createFDs(web,1);
    	System.out.println(String.format("Key propagation, %d initial keys", consolidatedKeys.size()));
    	
    	// run a blocker with schema correspondences that propagates the keys
    	KeyBySchemaCorrespondenceBlocker<MatchableTableRow> keyBlocker = null;
    	if(noEarlyFiltering) {
    		// takes care of multiple possible attribute assignments between two tables (i.e., no 1:1 mapping between attributes)
    		keyBlocker = new KeyByMultiSchemaCorrespondenceBlocker<>(true);
    	} else {
    		keyBlocker = new KeyBySchemaCorrespondenceBlocker<>(true);
    	}
    	keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    	
    	// then consolidate the created keys, i.e., create a dataset with the new keys
    	CandidateKeyConsolidator<MatchableTableColumn> keyConsolidator = new CandidateKeyConsolidator<>(true);
    	consolidatedKeys = keyConsolidator.run(new ResultSet<MatchableTableKey>(), keyCorrespondences, proc);

    	System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", 1, consolidatedKeys.size(), keyCorrespondences.size()));
    	
    	int last = consolidatedKeys.size();
    	int round = 2;
    	do {
    		last = consolidatedKeys.size();
    		keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    		consolidatedKeys = keyConsolidator.run(new ResultSet<MatchableTableKey>(), keyCorrespondences, proc);
    		System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", round++, consolidatedKeys.size(), keyCorrespondences.size()));
    	} while(consolidatedKeys.size()!=last);

    	// filter out keys that only consist of context columns
    	Iterator<Correspondence<MatchableTableKey, MatchableTableColumn>> corIt = keyCorrespondences.get().iterator();
    	while(corIt.hasNext()) {
    		Correspondence<MatchableTableKey, MatchableTableColumn> cor = corIt.next();
    		if(Q.all(cor.getFirstRecord().getColumns(), new Func<Boolean, MatchableTableColumn>() {

				@Override
				public Boolean invoke(MatchableTableColumn in) {
					return ContextColumns.isContextColumn(in);
				}
			})) {
    			corIt.remove();
    		}
    	}
    	
    	
    	return keyCorrespondences;
	}

	protected ResultSet<MatchableTableKey> createFDs(WebTables web, int minDeterminantSize) {
		// create a dataset of FD left-hand sides
		ResultSet<MatchableTableKey> fds = new ResultSet<>();
		for(Table t : web.getTables().values()) {
			for(Collection<TableColumn> lhs : t.getSchema().getFunctionalDependencies().keySet()) {
				
				// make sure that the LHS is a determinant in the fd (and not {}->X)
				if(lhs.size()>0) {
				
					// do not consider trivial fds
					if(lhs.size()==1) {
						Collection<TableColumn> rhs = t.getSchema().getFunctionalDependencies().get(lhs);
						if(lhs.equals(rhs)) {
							continue;
						}
					} 
					
					if(lhs.size()<minDeterminantSize) {
						continue;
					}
					
					Set<MatchableTableColumn> columns = new HashSet<>();
					
					for(TableColumn c : lhs) {
						MatchableTableColumn col = web.getSchema().getRecord(c.getIdentifier());
						columns.add(col);
					}
					
					MatchableTableKey k = new MatchableTableKey(t.getTableId(), columns);
					fds.add(k);
				
//					System.out.println(String.format("#%d\t%s\t%s->%s", t.getTableId(), t.getPath(), lhs, t.getSchema().getFunctionalDependencies().get(lhs)));
				}
			}
		}
		System.out.println(String.format("%d non-trivial, minimal FDs", fds.size()));
		
		return fds;
	}
	
}
