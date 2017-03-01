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
import java.util.Iterator;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.rules.CandidateKeyConsolidator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.SchemaSynonymBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.InstanceByKeyToSchemaAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation.TrustedKeyAggregator;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyBasedBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyByMultiSchemaCorrespondenceBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyBySchemaCorrespondenceBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyCorrespondenceBasedBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.MatchingKeyRecordBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.SynonymBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.graph.ResolveConflictsByEdgeBetweenness;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.DisjointHeaderSchemaMatchingRule;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.KeyMatchingRefiner;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.PairwiseOneToOneMapping;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.SpecialColumnsSchemaFilter;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.parallel.ParallelMatchingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DuplicateBasedTableMatching extends TableMatchingTask {

	protected boolean useTrustedKeys = false;
	protected boolean useSchemaRefinement = false;
	protected boolean useKeyMatching = false;
	protected boolean useGraphOptimisation = false;
	protected boolean noEarlyFiltering = false;
	protected boolean keyMatchingRemovesUncertain = false;
	protected double minSimilarity = 1.0;
	protected boolean fullDeterminantMatchOnly = true;
	protected int minVotes = 0;
	
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask#getTaskName()
	 */
	@Override
	public String getTaskName() {
		String name = super.getTaskName();
		
		if(useTrustedKeys) {
			name += "+TrustedKeys";
		}
		if(useKeyMatching) {
			name += "+KeyMatching";
		}
		if(useSchemaRefinement) {
			name += "+SchemaRefinement";
		}
		if(useGraphOptimisation) {
			name += "+Graph";
		}
		if(noEarlyFiltering) {
			name += "+NoEarlyFilter";
		}
		if(keyMatchingRemovesUncertain) {
			name += "+RemoveUncertain";
		}
		if(fullDeterminantMatchOnly) {
			name += "+FullDeterminants";
		}
		if(minVotes>0) {
			name += "+min" + minVotes + "votes";
		}
		
		return name;
	}
	
	public static void main(String[] args) throws Exception {
		DuplicateBasedTableMatching tju = new DuplicateBasedTableMatching(false,false,false,false, false, false,1.0,true,0);

		if (tju.parseCommandLine(DuplicateBasedTableMatching.class, args)) {

			hello();

			tju.initialise();
			tju.setMatchingEngine(new ParallelMatchingEngine<MatchableTableRow, MatchableTableColumn>());
			tju.setDataProcessingEngine(tju.matchingEngine.getProcessingEngine());
			
			tju.match();

		}
	}

	
	
	/**
	 * @param useTrustedKeys
	 * @param useSchemaRefinement
	 * @param useKeyMatching
	 */
	public DuplicateBasedTableMatching(boolean useTrustedKeys, boolean useKeyMatching, boolean useSchemaRefinement, boolean useGraphOptimisation, boolean noEarlyFiltering, boolean keyMatchingRemovesUncertain, double minSimilarity, boolean fullDeterminantMatchOnly, int minVotes) {
		super();
		this.useTrustedKeys = useTrustedKeys;
		this.useSchemaRefinement = useSchemaRefinement;
		this.useKeyMatching = useKeyMatching;
		this.useGraphOptimisation = useGraphOptimisation;
		this.noEarlyFiltering = noEarlyFiltering;
		this.keyMatchingRemovesUncertain = keyMatchingRemovesUncertain;
		this.minSimilarity = minSimilarity;
		this.fullDeterminantMatchOnly = fullDeterminantMatchOnly;
		this.minVotes = minVotes;
	}



	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask#match()
	 */
	@Override
	public void runMatching() throws Exception {
    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = null;
    	keyInstanceCorrespondences = createRecordLinksFromKeys();
    	
    	if(useTrustedKeys) {
//	    	TrustedKeyAggregator trust = new TrustedKeyAggregator(1);
//	    	keyInstanceCorrespondences = trust.aggregate(keyInstanceCorrespondences, proc);
    		keyInstanceCorrespondences = applyTrustedKey(keyInstanceCorrespondences);
    	}
    	
    	DisjointHeaders dh = null;
    	if(useSchemaRefinement) {
    		dh = new DisjointHeaders(getDisjointHeaders());
    	} else {
    		dh = new DisjointHeaders(new HashMap<String, Set<String>>());
    	}
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = voteForSchema(keyInstanceCorrespondences, dh);

    	if(useKeyMatching) {
	    	/*********************************************** 
	    	 * match keys
	    	 ***********************************************/
	    	
	    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = matchKeys(schemaCorrespondences);
	
	    	/*********************************************** 
	    	 * create new record links based on matching keys
	    	 ***********************************************/
	    	
	    	MatchingKeyRecordBlocker matchingKeyBlocker = new MatchingKeyRecordBlocker();
	    	keyInstanceCorrespondences = matchingKeyBlocker.runBlocking(web.getRecords(), true, keyCorrespondences, proc);
	    	
	    	if(useTrustedKeys) {
	    		keyInstanceCorrespondences = applyTrustedKey(keyInstanceCorrespondences);
//		    	TrustedKeyAggregator trust = new TrustedKeyAggregator(1);
//		    	keyInstanceCorrespondences = trust.aggregate(keyInstanceCorrespondences, proc);
	    	}
	    	
	    	/*********************************************** 
	    	 * vote for schema correspondences
	    	 ***********************************************/
	    	
	    	schemaCorrespondences = voteForSchema(keyInstanceCorrespondences, dh);
    	}
    	
    	if(useSchemaRefinement) {
    		schemaCorrespondences = refineSchemaCorrespondences(schemaCorrespondences, dh);
    	}
    	
    	if(useGraphOptimisation) {
    		schemaCorrespondences = runGraphOptimisation(schemaCorrespondences);
    	}
    	
    	evaluateSchemaCorrespondences(schemaCorrespondences);
	}
	
	protected ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> applyTrustedKey(ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences) {
		ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> trustedKeyInstanceCorrespondences = null;
		
		TrustedKeyAggregator trust = new TrustedKeyAggregator(2);
		trustedKeyInstanceCorrespondences = trust.aggregate(keyInstanceCorrespondences, proc);
		
		return trustedKeyInstanceCorrespondences;
		
		//TODO re-enable this (adaptive trust) for overall better results
//    	if(trustedKeyInstanceCorrespondences.size()==0){
//    		//TODO check: does trusted key with 1 make any difference?
//    		trust = new TrustedKeyAggregator(1);
//        	return trust.aggregate(keyInstanceCorrespondences, proc);	
//    	} else{
//    		return trustedKeyInstanceCorrespondences;
//    	}
	}
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> runGraphOptimisation(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		ResolveConflictsByEdgeBetweenness rb = new ResolveConflictsByEdgeBetweenness(true);
		schemaCorrespondences.deduplicate();
		schemaCorrespondences = rb.match(schemaCorrespondences, proc, new DisjointHeaders(getDisjointHeaders()));
		return schemaCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> createRecordLinksFromKeys() {
    	// creates instance correspondences between the tables by matching the values of candidate keys
    	KeyBasedBlocker blocker = new KeyBasedBlocker();
    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = blocker.runBlocking(web.getRecords(), true, null, proc);
    	
    	// if the blocker linked two records, check all possible key correspondences and create key matches if possible
    	// - this is already a key propagation: if the key on one side matches a key on the other side plus additional attributes, we would not have found that link in the blocker
    	KeyMatchingRefiner keyRefiner = new KeyMatchingRefiner(1.0);
    	keyInstanceCorrespondences = keyRefiner.run(keyInstanceCorrespondences, proc);
    	
    	return keyInstanceCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> createRecordLinksFromDependencies() {
		ResultSet<MatchableTableKey> fds = createFDs(1);
		ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> fdCors = new ResultSet<>();
		for(MatchableTableKey k : fds.get()) {
			Correspondence<MatchableTableKey, MatchableTableColumn> c = new Correspondence<MatchableTableKey, MatchableTableColumn>(k, null, 0.0, null);
			fdCors.add(c);
		}
		KeyCorrespondenceBasedBlocker blocker = new KeyCorrespondenceBasedBlocker();
    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = blocker.runBlocking(web.getRecords(), true, fdCors, proc);
    	
    	// if the blocker linked two records, check all possible key correspondences and create key matches if possible
    	// - this is already a key propagation: if the key on one side matches a key on the other side plus additional attributes, we would not have found that link in the blocker
//    	KeyMatchingRefiner keyRefiner = new KeyMatchingRefiner(1.0);
//    	keyInstanceCorrespondences = keyRefiner.run(keyInstanceCorrespondences, proc);
    	
    	return keyInstanceCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> voteForSchema(
			ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences, 
			DisjointHeaders dh) {
		//TODO test differences if the last parameter is set to true!
//    	InstanceByKeyToSchemaAggregator instanceVoteForSchema = new InstanceByKeyToSchemaAggregator(2, noEarlyFiltering, false, false);
//		InstanceByKeyToSchemaAggregator instanceVoteForSchema = new InstanceByKeyToSchemaAggregator(minVotes, noEarlyFiltering, false, false, minSimilarity);
		InstanceByKeyToSchemaAggregator instanceVoteForSchema = new InstanceByKeyToSchemaAggregator(minVotes, false, false, false, minSimilarity);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences  = instanceVoteForSchema.aggregate(keyInstanceCorrespondences, proc);
    	
		DisjointHeaderSchemaMatchingRule disjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
		schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
		
    	return schemaCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> filterSchemaCorrespondencesBasedOnDeterminants(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> initialCorrespondences, ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences) {
		// generate all uncertain schema correspondences with duplicate-based matching
		InstanceByKeyToSchemaAggregator instanceVoteForSchema = new InstanceByKeyToSchemaAggregator(2, noEarlyFiltering, true, false, 1.0);
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences  = instanceVoteForSchema.aggregate(keyInstanceCorrespondences, proc);
    	
    	System.out.println(String.format("%d uncertain schema correspondences", schemaCorrespondences.size()));
    	
    	// as the correspondences are uncertain, not all duplicates voted for them
    	// which is why we remove them
    	for(Correspondence<MatchableTableColumn, MatchableTableRow> uncertain : schemaCorrespondences.get()) {
    		System.out.println(String.format("[uncertain] %s<->%s", uncertain.getFirstRecord(), uncertain.getSecondRecord()));
    		initialCorrespondences.remove(uncertain);
    	}
    	
    	// all other correspondences are kept
    	// - those for which all duplicates voted (we trust them)
    	// - those for which no duplicates were found (we cannot check them)
    	
    	return initialCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> matchKeys(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = null;
    	
    	BasicCollection<MatchableTableKey> consolidatedKeys = web.getCandidateKeys();
    	
    	System.out.println(String.format("Key propagation, %d initial keys", consolidatedKeys.size()));
    	// run a blocker with schema correspondences that propagates the keys
    	KeyBySchemaCorrespondenceBlocker<MatchableTableRow> keyBlocker = new KeyBySchemaCorrespondenceBlocker<>(true);
//    	SubkeyBySchemaCorrespondenceBlocker<MatchableTableRow> keyBlocker = new SubkeyBySchemaCorrespondenceBlocker<>(2);
    	keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    	// then consolidate the created keys, i.e., create a dataset with the new keys
    	CandidateKeyConsolidator<MatchableTableColumn> keyConsolidator = new CandidateKeyConsolidator<>(true);
//    	CandidateKeyConsolidator<MatchableTableColumn> keyConsolidator = new CandidateKeyConsolidator<>(false);
    	consolidatedKeys = keyConsolidator.run(web.getCandidateKeys(), keyCorrespondences, proc);
    	System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", 1, consolidatedKeys.size(), keyCorrespondences.size()));
    	
    	int last = consolidatedKeys.size();
    	int round = 2;
    	do {
    		last = consolidatedKeys.size();
    		keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    		consolidatedKeys = keyConsolidator.run(consolidatedKeys, keyCorrespondences, proc);
    		System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", round++, consolidatedKeys.size(), keyCorrespondences.size()));
    	} while(consolidatedKeys.size()!=last);
    	
    	return keyCorrespondences;
	}
	
	protected ResultSet<MatchableTableKey> createFDs(int minDeterminantSize) {
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
	
	protected ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> matchKeysByDependenciesFromSchemaCorrespondences(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		
		//TODO current problem: if fullDeterminantMatchOnly is false, the key propagation step creates a huge amount of keys (and iterations a lot of times...)
		
    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = null;
    	
    	// initialise the matching keys with the FDs that were discovered on the union tables
//    	ResultSet<MatchableTableKey> consolidatedKeys = createFDs(2);
    	ResultSet<MatchableTableKey> consolidatedKeys = createFDs(0);
    	System.out.println(String.format("Key propagation, %d initial keys", consolidatedKeys.size()));
    	
    	// run a blocker with schema correspondences that propagates the keys
    	KeyBySchemaCorrespondenceBlocker<MatchableTableRow> keyBlocker = null;
    	if(noEarlyFiltering) {
    		// takes care of multiple possible attribute assignments between two tables (i.e., no 1:1 mapping between attributes)
    		keyBlocker = new KeyByMultiSchemaCorrespondenceBlocker<>(fullDeterminantMatchOnly);
    	} else {
//    		keyBlocker = new KeyBySchemaCorrespondenceBlocker<>(true);
    		keyBlocker = new KeyBySchemaCorrespondenceBlocker<>(fullDeterminantMatchOnly);
    	}
    	keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    	
    	// then consolidate the created keys, i.e., create a dataset with the new keys
    	CandidateKeyConsolidator<MatchableTableColumn> keyConsolidator = new CandidateKeyConsolidator<>(true);
    	consolidatedKeys = keyConsolidator.run(new ResultSet<MatchableTableKey>(), keyCorrespondences, proc);

    	System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", 1, consolidatedKeys.size(), keyCorrespondences.size()));
//    	logKeyMatrixPerHeader(keyCorrespondences.get());
    	
//    	if(fullDeterminantMatchOnly) {
	    	int last = consolidatedKeys.size();
	    	int round = 2;
	    	do {
	    		last = consolidatedKeys.size();
	    		keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
	    		//TODO candidate key-based method consolidates with current set of keys instead of an empty set ...
	    		consolidatedKeys = keyConsolidator.run(new ResultSet<MatchableTableKey>(), keyCorrespondences, proc);
//	    		consolidatedKeys = keyConsolidator.run(consolidatedKeys, keyCorrespondences, proc);
	    		System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", round++, consolidatedKeys.size(), keyCorrespondences.size()));
//	    		logKeyMatrixPerHeader(keyCorrespondences.get());
	    	} while(consolidatedKeys.size()!=last);
//    	}
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
    	
    	// test subsets to find incorrect correspondences from baseline matcher
//    	consolidatedKeys = keyConsolidator.run(new ResultSet<MatchableTableKey>(), keyCorrespondences, proc);
//    	ResultSet<MatchableTableKey> subkeys = new ResultSet<>();
//    	for(MatchableTableKey key : consolidatedKeys.get()) {
//    		if(key.getTableId()==23 || key.getTableId()==29) {
//	    		for(MatchableTableColumn excluded : key.getColumns()) {
//	    			HashSet<MatchableTableColumn> columns = new HashSet<>(key.getColumns());
//	    			columns.remove(excluded);
//	    			MatchableTableKey subkey = new MatchableTableKey(key.getTableId(), columns);
//	    			subkeys.add(subkey);
//	    		}
//    		} else {
//    			subkeys.add(key);
//    		}
//    	}
//    	keyCorrespondences = keyBlocker.runBlocking(subkeys, true, schemaCorrespondences, proc);
    	
    	
		
    	
    	return keyCorrespondences;
//    	
//    	
//		
//    	KeyBySchemaCorrespondenceBlocker<MatchableTableRow> keyBlocker = new KeyBySchemaCorrespondenceBlocker<>(true);
//    	ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences = keyBlocker.runBlocking(fds, true, schemaCorrespondences, proc);
//
//    	System.out.println(String.format("%d matching keys based on FDs", keyCorrespondences.size()));
//    	
//		// remove determinants which are subsumed by others (this can lead to missing record links later ...)
//		CandidateKeyConsolidator<MatchableTableColumn> keyConsolidator = new CandidateKeyConsolidator<>(true);
//		fds = keyConsolidator.run(fds, new ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>>(), proc);
//		System.out.println(String.format("%d LHS's after consolidation", fds.size()));
//		
//		keyCorrespondences = keyBlocker.runBlocking(fds, true, schemaCorrespondences, proc);
//		System.out.println(String.format("%d matching keys based on FDs", keyCorrespondences.size()));
//
//		KeyBySchemaCorrespondenceBlocker<MatchableTableRow> keyBlocker = new KeyBySchemaCorrespondenceBlocker<>();
//    	keyCorrespondences = keyBlocker.runBlocking(fds, true, schemaCorrespondences, proc);
//		
//    	
//		MatchingKeyBySchemaCorrespondenceAndDependencyBlocker<MatchableTableRow> fdBlocker = new MatchingKeyBySchemaCorrespondenceAndDependencyBlocker<>();
//		ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCors = fdBlocker.runBlocking(fds, true, schemaCorrespondences, proc);
		
//		return keyCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> consolidateKeyCorrespondences(ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> keyCorrespondences, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		System.out.println(String.format("%d column combinations to check", keyCorrespondences.size()));
		
		CandidateKeyConsolidator<MatchableTableColumn> keyConsolidator = new CandidateKeyConsolidator<>();
    	ResultSet<MatchableTableKey> consolidatedKeys = keyConsolidator.run(new ResultSet<MatchableTableKey>(), keyCorrespondences, proc);
		
    	System.out.println(String.format("%d column combinations to check after consolidation", keyCorrespondences.size()));
    	
    	KeyBySchemaCorrespondenceBlocker<MatchableTableRow> keyBlocker = new KeyBySchemaCorrespondenceBlocker<>(false);
    	keyCorrespondences = keyBlocker.runBlocking(consolidatedKeys, true, schemaCorrespondences, proc);
    	
    	System.out.println(String.format("%d column combinations to check after propagation", keyCorrespondences.size()));
    	
    	keyCorrespondences.deduplicate();
    	System.out.println(String.format("%d column combinations (deduplicated)", keyCorrespondences.size()));
    	
    	return keyCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> refineSchemaCorrespondences(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, DisjointHeaders dh) {
//		DisjointHeaderSchemaMatchingRule disjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
//		
//    	// refine schema correspondences by applying disjoint header rule
////    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
//		
//    	// add schema correspondences via attribute names
//    	SchemaSynonymBlocker generateSynonyms = new SchemaSynonymBlocker();
//    	ResultSet<Set<String>> synonyms = generateSynonyms.runBlocking(web.getSchema(), true, schemaCorrespondences, proc);    	
//    	dh.extendWithSynonyms(synonyms.get());
//    	SynonymBasedSchemaBlocker synonymBlocker = new SynonymBasedSchemaBlocker(synonyms);
//    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondencesFromSynonyms = synonymBlocker.runBlocking(web.getSchema(), true, null, proc);    	
//    	schemaCorrespondencesFromSynonyms.deduplicate();
//    	schemaCorrespondencesFromSynonyms = disjointHeaderRule.run(schemaCorrespondencesFromSynonyms, proc);
//    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondencesFromSynonyms.get()) {
//    		schemaCorrespondences.add(cor);
//    	}
//    	schemaCorrespondences.deduplicate();
//    	
////    	N2NGoldStandard schemaMapping = createMappingForOriginalColumns(schemaCorrespondences.get());
////    	ClusteringPerformance schemaPerformance = gs.evaluateCorrespondenceClusters(schemaMapping.getCorrespondenceClusters(), false);
////    	System.out.println(gs.formatEvaluationResult(schemaPerformance.getPerformanceByCluster(), true));
//    	
//    	// remove special columns from schema correspondences (synonymBlocker is a blocker and uses all columns, so new correspondences for special columns can be created) 
//    	SpecialColumnsSchemaFilter specialColumnsFilter = new SpecialColumnsSchemaFilter();
//    	schemaCorrespondences = specialColumnsFilter.run(schemaCorrespondences, proc);
//    	
//    	// refine schema correspondences by applying disjoint header rule
//    	schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);

		Set<Set<String>> s = new HashSet<Set<String>>();
		for(MatchableTableColumn c : web.getSchema().get()) {
			if(!c.getHeader().equals("null")) {
				s.add(Q.toSet(c.getHeader()));
			}
		}
		
//		for(Set<String> syn : s) {
//			System.out.println(syn.toString());
//		}
		
		ResultSet<Set<String>> synonyms = new ResultSet<>(s);
		SynonymBasedSchemaBlocker synonymBlocker = new SynonymBasedSchemaBlocker(synonyms);
//		log(String.format("Synonym-based schema matching for %d schema elements", web.getSchema().size()));
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> synonymCorrespondences = synonymBlocker.runBlocking(web.getSchema(), true, null, proc);
//		log(String.format("Merged %d synonym-based and %d voting-based schema correspondences", synonymCorrespondences.size(), schemaCorrespondences.size()));
    	
//    	for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : synonymCorrespondences.get()) {
//    		System.out.println(String.format("%s <-> %s", cor.getFirstRecord(), cor.getSecondRecord()));
//    	}
    	
    	// fix for cases in which a tables contains the same header multiple times: create a one-to-one mapping
    	PairwiseOneToOneMapping oneToOne = new PairwiseOneToOneMapping();
    	synonymCorrespondences = oneToOne.run(synonymCorrespondences, proc);
    	
    	schemaCorrespondences = proc.append(schemaCorrespondences, synonymCorrespondences);
    	
    	return schemaCorrespondences;
	}
}
