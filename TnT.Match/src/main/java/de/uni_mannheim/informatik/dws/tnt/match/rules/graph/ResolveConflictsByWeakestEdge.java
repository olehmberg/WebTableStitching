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
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
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
public class ResolveConflictsByWeakestEdge {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> match(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, DataProcessingEngine proc) {
		
		boolean changed = false;
		do {
			// find conflicts
			// create connected components
			ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
			for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
				clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
			}
			
			Map<Collection<MatchableTableColumn>, MatchableTableColumn> clusters = clusterer.createResult();
			// count table frequency
			for(Collection<MatchableTableColumn> clu : clusters.keySet()) {
				Distribution<Integer> tableFrequencies = Distribution.fromCollection(clu, new Func<Integer, MatchableTableColumn>() {
	
					@Override
					public Integer invoke(MatchableTableColumn in) {
						return in.getTableId();
					}
				});
				
				// table -> freq
//				Map<Integer, Integer> conflicts = new HashMap<>();
				
				for(Integer table : tableFrequencies.getElements()) {
					int freq = tableFrequencies.getFrequency(table);
					
					if(freq>1) {
//						conflicts.put(table, freq);
						
						List<Correspondence<MatchableTableColumn, MatchableTableRow>> path = findPath(new HashSet<>(schemaCorrespondences.get()), table);
						Correspondence<MatchableTableColumn, MatchableTableRow> weakest = null;
						
						for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : path) {
							if(weakest==null || cor.getSimilarityScore()<weakest.getSimilarityScore()) {
								weakest = cor;
							}
						}
						schemaCorrespondences.remove(weakest);
						System.out.println(String.format("[Removed Conflict] %s<->%s (%.4f)", weakest.getFirstRecord(), weakest.getSecondRecord(), weakest.getSimilarityScore()));
						changed=true;
						break;
					}
				}
				
				// resolve conflicts
				// get a path between two columns of the same table and remove the weakest edge
			}
		
		} while(changed); // very inefficient way of looping until all conflicts are resolved
		
		return schemaCorrespondences;
	}

	protected Set<Correspondence<MatchableTableColumn, MatchableTableRow>> getCorrespondencesForCluster(Set<MatchableTableColumn> cluster, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences) {
		HashSet<Correspondence<MatchableTableColumn, MatchableTableRow>> result = new HashSet<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : correspondences.get()) {
			if(cluster.contains(cor.getFirstRecord()) && cluster.contains(cor.getSecondRecord())) {
				result.add(cor);
			}
		}
		
		return result;
	}
	
	protected List<Correspondence<MatchableTableColumn, MatchableTableRow>> findPath(Set<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences, int conflictingTableId) {
		
		Map<MatchableTableColumn, Set<Correspondence<MatchableTableColumn, MatchableTableRow>>> adjacency = new HashMap<>();
		Set<MatchableTableColumn> conflictingCols = new HashSet<>();
		// determine two columns from the same table
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : correspondences) {
			Set<Correspondence<MatchableTableColumn, MatchableTableRow>> links = MapUtils.get(adjacency, cor.getFirstRecord(), new HashSet<Correspondence<MatchableTableColumn, MatchableTableRow>>());
			links.add(cor);
			
			links = MapUtils.get(adjacency, cor.getSecondRecord(), new HashSet<Correspondence<MatchableTableColumn, MatchableTableRow>>());
			links.add(cor);
			
			if(cor.getFirstRecord().getTableId()==conflictingTableId) {
				conflictingCols.add(cor.getFirstRecord());
			}
			if(cor.getSecondRecord().getTableId()==conflictingTableId) {
				conflictingCols.add(cor.getSecondRecord());
			}
		}
		
		MatchableTableColumn col1 = Q.firstOrDefault(conflictingCols);
		conflictingCols.remove(col1);
		MatchableTableColumn col2 = Q.firstOrDefault(conflictingCols);
		
		return search(col1, col2, adjacency);
	}
	
	protected List<Correspondence<MatchableTableColumn, MatchableTableRow>> search(MatchableTableColumn col1, MatchableTableColumn col2, Map<MatchableTableColumn, Set<Correspondence<MatchableTableColumn, MatchableTableRow>>> adjacency) {
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor :adjacency.get(col1)) {
			MatchableTableColumn otherCol = null;
			if(cor.getFirstRecord().equals(col1)) {
				otherCol = cor.getSecondRecord();
			} else {
				otherCol = cor.getFirstRecord();
			}
			
			if(otherCol.equals(col2)) {
				return Q.toList(cor);
			} else {
				List<Correspondence<MatchableTableColumn, MatchableTableRow>> result = search(otherCol, col2, adjacency);
				
				if(result!=null) {
					result.add(cor);
					return result;
				}
			}
		}
		
		return null;
		
	}
}
