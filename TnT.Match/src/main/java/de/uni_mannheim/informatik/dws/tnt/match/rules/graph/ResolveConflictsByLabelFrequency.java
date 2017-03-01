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
package de.uni_mannheim.informatik.dws.tnt.match.rules.graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * Removes inconsistencies from a schema mapping by enforcing that no two attribute clusters can contain the same attribute name.
 * 
 * In case of a conflict, the attribute name is removed from the cluster where it appears the least frequent.
 * 
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ResolveConflictsByLabelFrequency {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> match(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, DataProcessingEngine proc) {
		
		// create the attribute clusters
		HashSet<MatchableTableColumn> nodes = new HashSet<>();
		ConnectedComponentClusterer<MatchableTableColumn> comp = new ConnectedComponentClusterer<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			if(cor.getSimilarityScore()>0.0) {
				nodes.add(cor.getFirstRecord());
				nodes.add(cor.getSecondRecord());
				
				comp.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
			}
		}
		
		
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clusters = comp.createResult();
		
		// count the frequency of clusters for each attribute name
		
		// map attribute name -> set of clusters
		Map<String, Set<Collection<MatchableTableColumn>>> labelToCluster = new HashMap<>();
		
		for(Collection<MatchableTableColumn> clu : clusters.keySet()) {
			for(MatchableTableColumn c : clu) {
				if(!c.getHeader().equals("null")) {
					Set<Collection<MatchableTableColumn>> cluForLabel = MapUtils.get(labelToCluster, c.getHeader(), new HashSet<Collection<MatchableTableColumn>>());
					cluForLabel.add(clu);
				}
			}
			
		}
		
		// resolve conflicts
		for(String label : labelToCluster.keySet()) {
			Set<Collection<MatchableTableColumn>> cluForLabel = labelToCluster.get(label);
			
			if(cluForLabel.size()>1) {
				// map cluster to label frequency
				Map<Collection<MatchableTableColumn>, Integer> cluToLabelFreq = new HashMap<>();
				
				for(Collection<MatchableTableColumn> clu : cluForLabel) {
					Distribution<String> freqs = Distribution.fromCollection(clu, new MatchableTableColumn.ColumnHeaderProjection());
					
					cluToLabelFreq.put(clu, freqs.getFrequency(label));
				}
				
				Collection<MatchableTableColumn> max = MapUtils.max(cluToLabelFreq);
				
				// remove schema correspondences from other clusters than max
				Iterator<Correspondence<MatchableTableColumn, MatchableTableRow>> corIt = schemaCorrespondences.get().iterator();
				
				while(corIt.hasNext()) {
					Correspondence<MatchableTableColumn, MatchableTableRow> cor = corIt.next();
					
					MatchableTableColumn colWithLabel = null;
					if(cor.getFirstRecord().getHeader().equals(label)) {
						colWithLabel = cor.getFirstRecord();
					} else if(cor.getSecondRecord().getHeader().equals(label)) {
						colWithLabel = cor.getSecondRecord();
					}
					
					if(colWithLabel!=null) {
						if(!max.contains(colWithLabel)) {
							corIt.remove();
						}
					}
				}
			}
		}
		
		return schemaCorrespondences;
	}
	
}
