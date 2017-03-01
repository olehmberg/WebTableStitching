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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.rules.blocking.SynonymBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.tnt.match.rules.graph.ResolveConflictsByEdgeBetweenness;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.DisjointHeaderSchemaMatchingRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class OneClusterMatcher extends TableMatchingTask {

	protected boolean useSchemaRefinement = false;
	protected boolean useGraphOptimisation = false;
	
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask#getTaskName()
	 */
	@Override
	public String getTaskName() {
		String name = super.getTaskName();

		if(useSchemaRefinement) {
			name += "+SchemaRefinement";
		}
		if(useGraphOptimisation) {
			name += "+Graph";
		}
		
		return name;
	}
	
	public OneClusterMatcher(boolean useSchemaRefinement, boolean useGraphOptimisation) {
		this.useSchemaRefinement = useSchemaRefinement;
		this.useGraphOptimisation = useGraphOptimisation;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask#runMatching()
	 */
	@Override
	public void runMatching() throws Exception {

		Set<String> baseline = new HashSet<>();
		for(Set<String> clu : unionGs.getCorrespondenceClusters().keySet()) {
			baseline.addAll(clu);
		}
		
		// create baseline for all attributes in one cluster
		Map<Set<String>, String> map = new HashMap<>();
		map.put(baseline, "all");
		
		ClusteringPerformance baselineResult = unionGs.evaluateCorrespondenceClusters(map, true);
		unionSchemaPerformance = baselineResult;
    	
    	Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> cors = new LinkedList<>();
    	List<String> columns = new ArrayList<>(baseline);
    	for(int i = 0; i < columns.size(); i++) {
    		for(int j = i + 1; j < columns.size(); j++) {
    			cors.add(new Correspondence<MatchableTableColumn, MatchableTableRow>(new MatchableTableColumn(columns.get(i)), new MatchableTableColumn(columns.get(j)), 1.0, null));
    		}
    	}
    	
    	DisjointHeaders dh = null;
    	if(useSchemaRefinement) {
    		dh = new DisjointHeaders(getDisjointHeaders());
    	} else {
    		dh = new DisjointHeaders(new HashMap<String, Set<String>>());
    	}
    	
    	if(useSchemaRefinement) {
    		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = new ResultSet<>(cors);
//    		schemaCorrespondences = refineSchemaCorrespondences(schemaCorrespondences, dh);
    		DisjointHeaderSchemaMatchingRule disjointHeaderRule = new DisjointHeaderSchemaMatchingRule(dh);
    		schemaCorrespondences = disjointHeaderRule.run(schemaCorrespondences, proc);
    		cors = schemaCorrespondences.get();
    	}
    	
    	if(useGraphOptimisation) {
    		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = new ResultSet<>(cors);
    		schemaCorrespondences = runGraphOptimisation(schemaCorrespondences);
    		cors = schemaCorrespondences.get();
    	}
    	
    	unionSchemaPerformance.setTransitiveCorrespondencePerformance(unionGs.evaluateCorrespondencePerformance(cors, false));
		
		baselineResult = unionGs.evaluateCorrespondenceClustersInverse(map.keySet(), false);
		unionSchemaPerformanceInverse = baselineResult;

	}

	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> refineSchemaCorrespondences(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, DisjointHeaders dh) {

		Set<Set<String>> s = new HashSet<Set<String>>();
		for(MatchableTableColumn c : web.getSchema().get()) {
			if(!c.getHeader().equals("null")) {
				s.add(Q.toSet(c.getHeader()));
			}
		}
		
		ResultSet<Set<String>> synonyms = new ResultSet<>(s);
		SynonymBasedSchemaBlocker synonymBlocker = new SynonymBasedSchemaBlocker(synonyms);
//		log(String.format("Synonym-based schema matching for %d schema elements", web.getSchema().size()));
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> synonymCorrespondences = synonymBlocker.runBlocking(web.getSchema(), true, null, proc);
//		log(String.format("Merged %d synonym-based and %d voting-based schema correspondences", synonymCorrespondences.size(), schemaCorrespondences.size()));
    	schemaCorrespondences = proc.append(schemaCorrespondences, synonymCorrespondences);
		
    	return schemaCorrespondences;
	}
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> runGraphOptimisation(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		ResolveConflictsByEdgeBetweenness rb = new ResolveConflictsByEdgeBetweenness(true);
		schemaCorrespondences.deduplicate();
		schemaCorrespondences = rb.match(schemaCorrespondences, proc, new DisjointHeaders(getDisjointHeaders()));
		return schemaCorrespondences;
	}
	
}
