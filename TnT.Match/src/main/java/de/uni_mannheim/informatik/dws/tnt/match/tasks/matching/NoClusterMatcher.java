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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.wdi.model.Correspondence;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class NoClusterMatcher extends TableMatchingTask {

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.tasks.matching.TableMatchingTask#runMatching()
	 */
	@Override
	public void runMatching() throws Exception {
		
		// create baseline for no result
		Set<String> baseline = new HashSet<>();
		Map<Set<String>, String> map = new HashMap<>();
		map.put(baseline, "all");
		
		ClusteringPerformance baselineResult = unionGs.evaluateCorrespondenceClusters(map, true);
		unionSchemaPerformance = baselineResult;

    	List<Correspondence<MatchableTableColumn, Object>> cors = new LinkedList<>();
    	List<String> columns = new ArrayList<>(baseline);
    	for(int i = 0; i < columns.size(); i++) {
    		for(int j = i + 1; j < columns.size(); j++) {
    			cors.add(new Correspondence<MatchableTableColumn, Object>(new MatchableTableColumn(columns.get(i)), new MatchableTableColumn(columns.get(j)), 1.0, null));
    		}
    	}
    	
    	unionSchemaPerformance.setTransitiveCorrespondencePerformance(unionGs.evaluateCorrespondencePerformance(cors, false));
		
		baselineResult = unionGs.evaluateCorrespondenceClustersInverse(map.keySet(), false);
		unionSchemaPerformanceInverse = baselineResult;
	}

}
