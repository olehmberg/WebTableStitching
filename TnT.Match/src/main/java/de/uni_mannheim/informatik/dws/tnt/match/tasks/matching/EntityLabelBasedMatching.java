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

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.KeyCorrespondenceBasedBlocker;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityLabelBasedMatching extends DuplicateBasedTableMatching {

	public EntityLabelBasedMatching(boolean useTrustedKeys, boolean useKeyMatching, boolean useSchemaRefinement,
			boolean useGraphOptimisation, boolean noEarlyFiltering, boolean keyMatchingRemovesUncertain, double minSimilarity, int minVotes) {
		super(useTrustedKeys, useKeyMatching, useSchemaRefinement, useGraphOptimisation, noEarlyFiltering,
				keyMatchingRemovesUncertain, minSimilarity, true, minVotes);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.DuplicateBasedTableMatching#runMatching()
	 */
	@Override
	public void runMatching() throws Exception {
//    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = null;
    	keyInstanceCorrespondences = createRecordLinksFromEntityLabels();
    	
    	DisjointHeaders dh = null;
    	if(useSchemaRefinement) {
    		dh = new DisjointHeaders(getDisjointHeaders());
    	} else {
    		dh = new DisjointHeaders(new HashMap<String, Set<String>>());
    	}
    	schemaCorrespondences = voteForSchema(keyInstanceCorrespondences, dh);
    	
    	if(useSchemaRefinement) {
    		schemaCorrespondences = refineSchemaCorrespondences(schemaCorrespondences, dh);
    	}
    	
    	if(useGraphOptimisation) {
    		schemaCorrespondences = runGraphOptimisation(schemaCorrespondences);
    	}
    	
    	evaluateSchemaCorrespondences(schemaCorrespondences);
	}

	protected ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> createRecordLinksFromEntityLabels() {
		ResultSet<Correspondence<MatchableTableKey, MatchableTableColumn>> entityLabelCorrespondence = new ResultSet<>();
//		keyCorrespondences = new ResultSet<>();
		
		for(Table t : web.getTables().values()) {
			if(!t.hasKey()) {
				t.inferSchema();
				t.identifyKey();
			}
			System.out.println(String.format("#%d %s Key: %s", t.getTableId(), t.getPath(), t.getKey()));
			MatchableTableKey k = new MatchableTableKey(t.getTableId(), Q.toSet(web.getSchema().getRecord(t.getKey().getIdentifier())));
			Correspondence<MatchableTableKey, MatchableTableColumn> c = new Correspondence<MatchableTableKey, MatchableTableColumn>(k, null, 0.0, null);
			entityLabelCorrespondence.add(c);
//			keyCorrespondences.add(c);
		}
		KeyCorrespondenceBasedBlocker blocker = new KeyCorrespondenceBasedBlocker();
    	ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = blocker.runBlocking(web.getRecords(), true, entityLabelCorrespondence, proc);
//		ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> keyInstanceCorrespondences = blocker.runBlocking(web.getRecords(), true, keyCorrespondences, proc);
    	
    	return keyInstanceCorrespondences;
	}
	
}
