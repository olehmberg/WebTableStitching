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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.rules.CandidateKeyConsolidator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaToColumnCombinationAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.SchemaToSingleColumnCombinationAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyBySchemaCorrespondenceBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyBySchemaCorrespondenceAndDependencyBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlockerImproved;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlockerImproved2;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.SubkeyBySchemaCorrespondenceBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.SynonymBasedSchemaBlocker;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class LabelBasedTableMatching extends DuplicateBasedTableMatching {


	/**
	 * @param useTrustedKeys
	 * @param useKeyMatching
	 * @param useSchemaRefinement
	 */
	public LabelBasedTableMatching(boolean useTrustedKeys, boolean useKeyMatching,
			boolean useSchemaRefinement, boolean useGraphOptimisation, boolean noEarlyFiltering, boolean keyMatchingRemovesUncertain) {
		super(useTrustedKeys, useKeyMatching, useSchemaRefinement, useGraphOptimisation, noEarlyFiltering, keyMatchingRemovesUncertain, 1.0, true,2);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask#match()
	 */
	@Override
	public void runMatching() throws Exception {
    	keyInstanceCorrespondences = null;
    	
		Set<Set<String>> s = new HashSet<Set<String>>();
		for(MatchableTableColumn c : web.getSchema().get()) {
			if(!c.getHeader().equals("null")) {
				s.add(Q.toSet(c.getHeader()));
			}
		}
		
		ResultSet<Set<String>> synonyms = new ResultSet<>(s);
		SynonymBasedSchemaBlocker synonymBlocker = new SynonymBasedSchemaBlocker(synonyms);
    	schemaCorrespondences = synonymBlocker.runBlocking(web.getSchema(), true, null, proc);  
    	
    	DisjointHeaders dh = null;
    	if(useSchemaRefinement) {
    		dh = new DisjointHeaders(getDisjointHeaders());
    	} else {
    		dh = new DisjointHeaders(new HashMap<String, Set<String>>());
    	}

    	if(useKeyMatching) {
	    	/*********************************************** 
	    	 * match keys
	    	 ***********************************************/
	    	
//    		SchemaToColumnCombinationAggregator schemaToKey = new SchemaToColumnCombinationAggregator(2);
//	    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = schemaToKey.aggregate(schemaCorrespondences, proc);
//	    	keyCorrespondences.deduplicate();
//    		
//	    	keyCorrespondences = consolidateKeyCorrespondences(keyCorrespondences, schemaCorrespondences);

    		keyCorrespondences = matchKeysByDependenciesFromSchemaCorrespondences(schemaCorrespondences);
	    	
        	
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
    	
    	evaluateSchemaCorrespondences(schemaCorrespondences);
	}
	

	
}
