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
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.rules.ValueBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceByDeterminantToSchemaAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceByKeyToSchemaAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.TrustedDeterminantAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlockerImproved2;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.DisjointHeaderSchemaMatchingRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ValueBasedTableMatching extends DuplicateBasedTableMatching {

	protected boolean linkViaDeterminants = false;
	protected int initialValueCountThreshold = 0;
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.DuplicateBasedTableMatching#getTaskName()
	 */
	@Override
	public String getTaskName() {
		String name = super.getTaskName();
		
		if(linkViaDeterminants) {
			name += "+LinkViaDeterminants";
		}
		if(initialValueCountThreshold>0) {
			name += "+ValueCount" + initialValueCountThreshold;
		}
				
		return name;
	}
	
	/**
	 * @param useTrustedKeys
	 * @param useKeyMatching
	 * @param useSchemaRefinement
	 */
	public ValueBasedTableMatching(boolean useTrustedKeys, boolean useKeyMatching,
			boolean useSchemaRefinement, boolean useGraphOptimisation, boolean noEarlyFiltering, boolean keyMatchingRemovesUncertain, boolean linkViaDeterminants, double minSimilarity, boolean fullDeterminantMatchOnly, int minVotes, int initialValueCountThreshold) {
		super(useTrustedKeys, useKeyMatching, useSchemaRefinement, useGraphOptimisation, noEarlyFiltering, keyMatchingRemovesUncertain,minSimilarity,fullDeterminantMatchOnly, minVotes);
		this.linkViaDeterminants = linkViaDeterminants;		
		this.initialValueCountThreshold = initialValueCountThreshold;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask#match()
	 */
	@Override
	public void runMatching() throws Exception {
    	ValueBasedSchemaBlocker valueBasedSchemaBlocker = new ValueBasedSchemaBlocker(noEarlyFiltering,initialValueCountThreshold);
    	schemaCorrespondences = valueBasedSchemaBlocker.runBlocking(web.getRecords(), true, null, proc);  
    	
    	System.out.println("Initial, value-based mapping");
    	logMatrixPerHeader(schemaCorrespondences.get());
    	
//    	System.out.println(String.format("[DeterminantFilter] before: %d correspondences", schemaCorrespondences.size()));
//    	DeterminantOnlyFilter df = new DeterminantOnlyFilter(web);
//    	schemaCorrespondences = df.run(schemaCorrespondences, proc);
//    	System.out.println(String.format("[DeterminantFilter] after: %d correspondences", schemaCorrespondences.size()));
    	
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
	    	
    		//TODO divide correspondences into two sets: ambiguous / non-ambiguous
    		// create keys based on ambiguous correspondence combinations, i.e. a<->b, a<->c, and check them via duplicate based matching
//    		SchemaToColumnCombinationAggregator schemaToKey = new SchemaToColumnCombinationAggregator(2);
//	    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = schemaToKey.aggregate(schemaCorrespondences, proc);
    		keyCorrespondences = null;
    		if(linkViaDeterminants) {
    			keyCorrespondences = matchKeysByDependenciesFromSchemaCorrespondences(schemaCorrespondences);
    		} else {
    			keyCorrespondences = matchKeys(schemaCorrespondences);
    		}
    
    		System.out.println("Determinant mapping");
    		logKeyMatrixPerHeader(keyCorrespondences.get());
    		System.out.println("Attribute mapping from determinants");
    		logMatrixPerHeaderFromKeys(keyCorrespondences.get());
	
//	    	keyCorrespondences = consolidateKeyCorrespondences(keyCorrespondences, schemaCorrespondences);
	
//	    	printKeyCorrespondences(keyCorrespondences.get());
    		
	    	/*********************************************** 
	    	 * create new record links based on matching keys
	    	 ***********************************************/
	    	
	    	MatchingKeyRecordBlockerImproved2 matchingKeyBlocker = new MatchingKeyRecordBlockerImproved2();
	    	keyInstanceCorrespondences = matchingKeyBlocker.runBlocking(web.getRecords(), true, keyCorrespondences, proc);
	    	
	    	if(useTrustedKeys) {
	    		int before = keyInstanceCorrespondences.size();
//	    		keyInstanceCorrespondences = applyTrustedKey(keyInstanceCorrespondences);
	    		TrustedDeterminantAggregator trust = new TrustedDeterminantAggregator(1);
	    		keyInstanceCorrespondences = trust.aggregate(keyInstanceCorrespondences, proc);
	    		System.out.println(String.format("[TrustedKey] %d/%d duplicates after filtering", keyInstanceCorrespondences.size(), before));
//	    		TrustedKeyAggregator trustedKey = new TrustedKeyAggregator(1);
//	        	keyInstanceCorrespondences = trustedKey.aggregate(keyInstanceCorrespondences, proc);
	    	}
	    	
	    	System.out.println("Determinant mapping by record links");
	    	logKeyMatrixPerHeaderFromRecordLinks(keyInstanceCorrespondences.get());
	    	
//	    	writeRecordLinkageGraph(new File(resultsLocationFile, "record_links_23_29.net"), keyInstanceCorrespondences, 23, 29);
//	    	writeRecordLinkageGraph(new File(resultsLocationFile, "record_links.net"), keyInstanceCorrespondences, -1, -1);
//	    	
//	    	MatchableTableRow r1 = web.getRecords().getRecord("42729.json~Row172200");
//	    	MatchableTableRow r2 = web.getRecords().getRecord("42730.json~Row21472");
//	    	MatchableTableRow r3 = web.getRecords().getRecord("42730.json~Row62434");
//	    	
//	    	System.out.println("####################################################################");
//	    	System.out.println(r1.format(20));
//	    	System.out.println(r2.format(20));
//	    	System.out.println(r3.format(20));
	    	
//    		TrustedDeterminantAggregator trust = new TrustedDeterminantAggregator(1, new File(resultsLocationFile, "temp"));
//    		trust.aggregate(keyInstanceCorrespondences, proc);
	    	
//	    	printKeyInstanceCorrespondences(keyInstanceCorrespondences, -1, -1);
	    	
	    	
	    	/*********************************************** 
	    	 * vote for schema correspondences
	    	 ***********************************************/
	    	
	    	if(keyMatchingRemovesUncertain) {
	    		schemaCorrespondences = filterSchemaCorrespondencesBasedOnDeterminants(schemaCorrespondences, keyInstanceCorrespondences);
	    	} else {
//	    		schemaCorrespondences = voteForSchema(keyInstanceCorrespondences, dh);
	    		schemaCorrespondences = voteForSchemaWithDeterminants(keyInstanceCorrespondences, schemaCorrespondences, dh);
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
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> voteForSchemaWithDeterminants(
			ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> determinantSchemaCorrespondences,
			DisjointHeaders dh) {

		InstanceByDeterminantToSchemaAggregator instanceVoteForSchema = new InstanceByDeterminantToSchemaAggregator(minVotes, false, false, false, minSimilarity);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences  = instanceVoteForSchema.aggregate(keyInstanceCorrespondences, determinantSchemaCorrespondences, proc);
    	
		DisjointHeaderSchemaMatchingRule disjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
		schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
		
    	return schemaCorrespondences;
	}
	
	
}
