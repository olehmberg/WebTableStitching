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
package de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaSynonymAggregator {

	public ResultSet<Set<String>> aggregate(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences,
			DataProcessingEngine proc) {
		
		ConnectedComponentClusterer<String> labelComponentClusterer = new ConnectedComponentClusterer<>();
		
		Set<String> allLabels = new HashSet<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : correspondences.get()) {
			
			String h1 = cor.getFirstRecord().getHeader();
			String h2 = cor.getSecondRecord().getHeader();
			
			allLabels.add(h1);
			allLabels.add(h2);
			
			if(!h1.equals(h2)) {
				labelComponentClusterer.addEdge(new Triple<String, String, Double>(h1, h2, 1.0));
			}
		}
		
		for(String label : allLabels) {
			labelComponentClusterer.addEdge(new Triple<String, String, Double>(label, label, 1.0));
		}
		
		ResultSet<Set<String>> result = new ResultSet<>();
		for(Collection<String> cluster : labelComponentClusterer.createResult().keySet()) {
			result.add(new HashSet<>(cluster));
		}
		
		return result;
	}
	
}
