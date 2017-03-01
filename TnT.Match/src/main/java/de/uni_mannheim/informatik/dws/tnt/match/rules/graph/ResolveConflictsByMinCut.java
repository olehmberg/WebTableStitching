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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.P;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.algorithms.flows.EdmondsKarpMaxFlow;
import edu.uci.ics.jung.algorithms.importance.BetweennessCentrality;
import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ResolveConflictsByMinCut {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> match(
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, 
			DataProcessingEngine proc,
			DisjointHeaders dh) {
		
		Graph<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> g = new UndirectedSparseGraph<>();
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {	
			g.addEdge(cor, cor.getFirstRecord(), cor.getSecondRecord());
		}
		
		WeakComponentClusterer<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> comp = new WeakComponentClusterer<>();
		Set<Set<MatchableTableColumn>> clusters = comp.apply(g);
		
		
		Queue<Set<MatchableTableColumn>> clustersToCheck = new LinkedList<>(clusters); 
		
		// generate conflicts in clusters
		while(clustersToCheck.size()>0) {
			Collection<MatchableTableColumn> cluster = clustersToCheck.poll();
			
			Pair<MatchableTableColumn, MatchableTableColumn> c = findConflict(cluster, dh);
			
//			EdmondsKarpMaxFlow<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> maxFlow = new EdmondsKarpMaxFlow<MatchableTableColumn, Correspondence<MatchableTableColumn,MatchableTableRow>>(g, c.getFirst(), c.getSecond(), arg3, arg4, arg5)
			
			if(c!=null) {
				// then remove the edges with the highest betweenness until the cluster breaks into two clusters to resolve the conflict				
				// only remove edges on paths between the nodes that make the conflict
				DijkstraShortestPath<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> dijkstra = new DijkstraShortestPath<>(g);
				
				List<Correspondence<MatchableTableColumn, MatchableTableRow>> path = null;
				
				do {
					path = dijkstra.getPath(c.getFirst(), c.getSecond());
					
					Correspondence<MatchableTableColumn, MatchableTableRow> maxEdge = null;
					double maxValue = Double.MIN_VALUE;
					
					for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : path) {
						
						if(cluster.contains(cor.getFirstRecord())) {
							
//							double value = between.getEdgeRankScore(cor);
							
//							if(value>maxValue) {
//								maxValue = value;
//								maxEdge = cor;
//							}
							
						}
						
					}
					
					System.out.println(String.format("[conflict] Removed %s<->%s", maxEdge.getFirstRecord(), maxEdge.getSecondRecord()));
					schemaCorrespondences.remove(maxEdge);
					g.removeEdge(maxEdge);
					
					try {
						path = dijkstra.getPath(c.getFirst(), c.getSecond());
					} catch(Exception ex) {
						path = null;
					}
					
				} while(path!=null); // repeat until there is no conflicting path left
				
				// re-cluster and add the new clusters to the queue
				Set<Set<MatchableTableColumn>> newClusters = comp.apply(g);
				for(Set<MatchableTableColumn> clu : newClusters) {
					if(Q.any(clu, new P.IsContainedIn<>(cluster))) {
						clustersToCheck.add(clu);
					}
				}
			}
		}
		

		// idea: the more paths we can find between nodes, the less probable is a coincidence
		// if there is only a single bridge in the graph between the conflicting nodes, then removing this bridge is a good idea
		
		// generate conflicts by: disjoint headers, same table (subset of disjoint headers)
		
		
		return schemaCorrespondences;
		
	}
	
	protected static class Conflict implements Comparable<Conflict> {
		public String header1;
		public String header2;
		public int count;
		
		public Conflict(String header1, String header2, int count) {
			this.header1 = header1;
			this.header2 = header2;
			this.count = count;
		}

		/* (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(Conflict o) {
			return Integer.compare(count, o.count);
		}
	}
	
	protected Pair<MatchableTableColumn, MatchableTableColumn> findConflict(Collection<MatchableTableColumn> cluster, DisjointHeaders dh) {
		
		Map<String, Collection<MatchableTableColumn>> groups = Q.group(cluster, new MatchableTableColumn.ColumnHeaderProjection());
		
		PriorityQueue<Conflict> conflicts = new PriorityQueue<>();
		List<String> headers = new ArrayList<>(groups.keySet());
		
		for(int i = 0; i < headers.size(); i++) {
			String header1 = headers.get(i);
			for(int j = i+1; j < headers.size(); j++) {
				String header2 = headers.get(j);
				
				if(dh.getDisjointHeaders(header1).contains(header2)) {
					Conflict c = new Conflict(header1, header2, groups.get(header1).size() * groups.get(header2).size());
					conflicts.add(c);
				}
			}
		}
		
		Conflict c = conflicts.poll();
		
		if(c==null) {
			return findConflictByTable(cluster);
		} else {
			return new Pair<MatchableTableColumn, MatchableTableColumn>(Q.firstOrDefault(groups.get(c.header1)), Q.firstOrDefault(groups.get(c.header2)));
		}
	}
	
	protected Pair<MatchableTableColumn, MatchableTableColumn> findConflictByTable(Collection<MatchableTableColumn> cluster) {
		Map<Integer, MatchableTableColumn> tableToColumn = new HashMap<>();
		
		for(MatchableTableColumn c : cluster) {
			
			MatchableTableColumn conflict = tableToColumn.get(c.getTableId());
			
			if(conflict==null) {
				tableToColumn.put(c.getTableId(), c);
			} else {
				return new Pair<MatchableTableColumn, MatchableTableColumn>(conflict, c);
			}
			
		}
		
		return null;
	}
}
