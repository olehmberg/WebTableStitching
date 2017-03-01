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
package de.uni_mannheim.informatik.dws.tnt.match.tasks.matching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.rules.ValueBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlockerImproved2;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.SynonymBasedSchemaBlocker;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class AllBaselinesTableMatching extends DuplicateBasedTableMatching {


	/**
	 * @param useTrustedKeys
	 * @param useKeyMatching
	 * @param useSchemaRefinement
	 */
	public AllBaselinesTableMatching(boolean useTrustedKeys, boolean useKeyMatching,
			boolean useSchemaRefinement, boolean useGraphOptimisation, boolean noEarlyFiltering, boolean keyMatchingRemovesUncertain, double minSimilarity) {
		super(useTrustedKeys, useKeyMatching, useSchemaRefinement, useGraphOptimisation, noEarlyFiltering, keyMatchingRemovesUncertain,minSimilarity,true,2);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask#match()
	 */
	@Override
	public void runMatching() throws Exception {
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = new ResultSet<>();
    	
		Set<Set<String>> s = new HashSet<Set<String>>();
		for(MatchableTableColumn c : web.getSchema().get()) {
			if(!c.getHeader().equals("null") && !SpecialColumns.isSpecialColumn(c)) {
				s.add(Q.toSet(c.getHeader()));
			}
		}
    	DisjointHeaders dh = null;
    	if(useSchemaRefinement) {
    		dh = new DisjointHeaders(getDisjointHeaders());
    	} else {
    		dh = new DisjointHeaders(new HashMap<String, Set<String>>());
    	}
		
    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = createRecordLinksFromKeys();
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> duplicateBasedschemaCorrespondences = voteForSchema(keyInstanceCorrespondences, dh);
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : duplicateBasedschemaCorrespondences.get()) {
    		schemaCorrespondences.add(cor);
    	}
    	
		ResultSet<Set<String>> synonyms = new ResultSet<>(s);
		SynonymBasedSchemaBlocker synonymBlocker = new SynonymBasedSchemaBlocker(synonyms);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> labelBasedSchemaCorrespondences = synonymBlocker.runBlocking(web.getSchema(), true, null, proc);  
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : labelBasedSchemaCorrespondences.get()) {
    		schemaCorrespondences.add(cor);
    	}
    	
    	ValueBasedSchemaBlocker valueBasedSchemaBlocker = new ValueBasedSchemaBlocker();
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> valueBasedSchemaCorrespondences = valueBasedSchemaBlocker.runBlocking(web.getRecords(), true, null, proc);
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : valueBasedSchemaCorrespondences.get()) {
    		schemaCorrespondences.add(cor);
    	}
    	
    	if(useKeyMatching) {
	    	/*********************************************** 
	    	 * match keys
	    	 ***********************************************/

    		ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = matchKeysByDependenciesFromSchemaCorrespondences(schemaCorrespondences);
	    	
        	
	    	/*********************************************** 
	    	 * create new record links based on matching keys
	    	 ***********************************************/
	    	
	    	MatchingKeyRecordBlockerImproved2 matchingKeyBlocker = new MatchingKeyRecordBlockerImproved2();
	    	keyInstanceCorrespondences = matchingKeyBlocker.runBlocking(web.getRecords(), true, keyCorrespondences, proc);
	    	
	    	/*********************************************** 
	    	 * vote for schema correspondences
	    	 ***********************************************/
	    	
	    	if(keyMatchingRemovesUncertain) {
	    		schemaCorrespondences = filterSchemaCorrespondencesBasedOnDeterminants(schemaCorrespondences, keyInstanceCorrespondences);
	    	} else {
	    		schemaCorrespondences = voteForSchema(keyInstanceCorrespondences, dh);
	    	}
    	}
    	
    	if(useSchemaRefinement) {
    		schemaCorrespondences = refineSchemaCorrespondences(schemaCorrespondences, dh);
    	}
    	
    	if(useGraphOptimisation) {
    		schemaCorrespondences = runGraphOptimisation(schemaCorrespondences);
    	}
    	
    	evaluateSchemaCorrespondences(schemaCorrespondences);
	}

}
