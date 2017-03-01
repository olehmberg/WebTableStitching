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

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.t2k.utils.query.P;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;
import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.algorithms.importance.BetweennessCentrality;
import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ResolveConflictsByEdgeBetweenness {

	// when removing an edge, remove all other edges with the same labels, too
	private boolean useLabelPropagation = false;
	
	public ResolveConflictsByEdgeBetweenness(boolean useLabelPropagation) {
		this.useLabelPropagation = useLabelPropagation;
	}
	
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> match(
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, 
			DataProcessingEngine proc,
			final DisjointHeaders dh) {
		
		// runtime improvement: create one graph per connected component, process all components in parallel (calculate betweenness, remove conflicts)
		
		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clusters = clusterer.createResult();
		
		ResultSet<Pair<MatchableTableColumn, Integer>> clustering = new ResultSet<>();
		int cluIdx = 0;
		for(Collection<MatchableTableColumn> cluster : clusters.keySet()) {
			for(MatchableTableColumn c : cluster) {
				clustering.add(new Pair<MatchableTableColumn, Integer>(c, cluIdx));
			}
			cluIdx++;
		}
		
		Function<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> joinByLeftColumn = new Function<MatchableTableColumn, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public MatchableTableColumn execute(Correspondence<MatchableTableColumn, MatchableTableRow> input) {
				return input.getFirstRecord();
			}
		};
		Function<MatchableTableColumn, Pair<MatchableTableColumn, Integer>> joinByClusteredColumn = new Function<MatchableTableColumn, Pair<MatchableTableColumn,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public MatchableTableColumn execute(Pair<MatchableTableColumn, Integer> input) {
				return input.getFirst();
			}
		};
		ResultSet<Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<MatchableTableColumn, Integer>>> corsWithClusters = proc.joinMixedTypes(schemaCorrespondences, clustering, joinByLeftColumn, joinByClusteredColumn);
		
		RecordKeyValueMapper<Integer, Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<MatchableTableColumn, Integer>>, Correspondence<MatchableTableColumn, MatchableTableRow>> groupByCluster = new RecordKeyValueMapper<Integer, Pair<Correspondence<MatchableTableColumn,MatchableTableRow>,Pair<MatchableTableColumn,Integer>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<MatchableTableColumn, Integer>> record,
					DatasetIterator<Pair<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {

				resultCollector.next(new Pair<Integer, Correspondence<MatchableTableColumn,MatchableTableRow>>(record.getSecond().getSecond(), record.getFirst()));
				
			}
		};
		ResultSet<Group<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>>> groupedByCluster = proc.groupRecords(corsWithClusters, groupByCluster);
		
		RecordMapper<Group<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn, MatchableTableRow>> resolveConflictsTransformation = new RecordMapper<Group<Integer,Correspondence<MatchableTableColumn,MatchableTableRow>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {

				ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = record.getRecords();
				
				Graph<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> g = new UndirectedSparseGraph<>();
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
					g.addEdge(cor, cor.getFirstRecord(), cor.getSecondRecord());
				}
				
				WeakComponentClusterer<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> comp = new WeakComponentClusterer<>();
				Set<Set<MatchableTableColumn>> clusters = comp.apply(g);
				
				BetweennessCentrality<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> between = new BetweennessCentrality<>(g);
				
//				System.out.println("calculating betweenness centrality");
				between.setRemoveRankScoresOnFinalize(false);
				between.evaluate();
				
//				System.out.println("removing conflicts");
				Queue<Set<MatchableTableColumn>> clustersToCheck = new LinkedList<>(clusters); 
				
				// generate conflicts in clusters
				while(clustersToCheck.size()>0) {
					Collection<MatchableTableColumn> cluster = clustersToCheck.poll();
					
					Pair<MatchableTableColumn, MatchableTableColumn> c = findConflict(cluster, dh);
					
					if(c!=null) {
//					while(c!=null) {
						
						boolean isSameTable = c.getFirst().getTableId() == c.getSecond().getTableId();
						
						StringBuilder sb = new StringBuilder();
						sb.append(String.format("[conflict] Detected %s<->%s\n", c.getFirst(), c.getSecond()));
						
						// then remove the edges with the highest betweenness until the cluster breaks into two clusters to resolve the conflict				
						// only remove edges on paths between the nodes that make the conflict
						DijkstraShortestPath<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> dijkstra = new DijkstraShortestPath<>(g);
						
						List<Correspondence<MatchableTableColumn, MatchableTableRow>> path = dijkstra.getPath(c.getFirst(), c.getSecond());
						
						do {
							sb.append(String.format("[conflict]\tShortest Path: %s\n", formatPath(path, between)));
							
							Correspondence<MatchableTableColumn, MatchableTableRow> maxEdge = null;
							double maxValue = Double.MIN_VALUE;
							
							// find the correspondence with the highest betweenness value
							for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : path) {
								
								// but don't remove a correspondence between columns with the same header, unless the conflict is in a single table
								String header1 = cor.getFirstRecord().getHeader();
								String header2 = cor.getSecondRecord().getHeader();
								boolean equalHeaders = header1!=null && header1.equals(header2);
								
								
								if(cluster.contains(cor.getFirstRecord()) && (!equalHeaders || isSameTable)) {
									
									double value = between.getEdgeRankScore(cor);
									
									if(value>maxValue) {
										maxValue = value;
										maxEdge = cor;
									}
									
								}
								
							}
							
							if(maxEdge!=null) {
								
//								schemaCorrespondences.remove(maxEdge);
//								g.removeEdge(maxEdge);
								removeEdge(schemaCorrespondences, g, maxEdge, sb);
								
								try {
									dijkstra = new DijkstraShortestPath<>(g);
									path = dijkstra.getPath(c.getFirst(), c.getSecond());
								} catch(Exception ex) {
									path = null;
								}
								
								if(path==null || path.size()==0) {
									sb.append("[conflict]\tResolved");
									path = null;
								}
							} else {
								sb.append("[conflict]\tCannot resolve");
								path = null;
							}
							
						} while(path!=null); // repeat until there is no conflicting path left
						
						System.out.println(sb.toString());
						
						// check if there is another conflict
//						c = findConflict(cluster, dh);
//						c = null;
						
						// re-cluster and add the new clusters to the queue
						Set<Set<MatchableTableColumn>> newClusters = comp.apply(g);
						for(Set<MatchableTableColumn> clu : newClusters) {
							// only clusters created from the current one can be changed, as we only removed edges from the current cluster
							if(Q.any(clu, new P.IsContainedIn<>(cluster)) && !cluster.equals(clu)) {
								clustersToCheck.add(clu);
							}
						}
					}
					
					
				}
				

				// idea: the more paths we can find between nodes, the less probable is a coincidence
				// if there is only a single bridge in the graph between the conflicting nodes, then removing this bridge is a good idea
				
				// generate conflicts by: disjoint headers, same table (subset of disjoint headers)
				
				
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
					resultCollector.next(cor);
				}
//				return schemaCorrespondences;	
			}
		};
		
		return proc.transform(groupedByCluster, resolveConflictsTransformation);
		
	}
	
	protected void removeEdge(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, Graph<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> g, Correspondence<MatchableTableColumn, MatchableTableRow> edge, StringBuilder sb) {
		
		if(!useLabelPropagation) {
			schemaCorrespondences.remove(edge);
			g.removeEdge(edge);
			sb.append(String.format("[conflict]\tRemoved %s<->%s\n", edge.getFirstRecord(), edge.getSecondRecord()));
		} else {
			String h1 = edge.getFirstRecord().getHeader();
			String h2 = edge.getSecondRecord().getHeader();
			
			Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> toRemove = new LinkedList<>();
			
			if(h1.equals(h2)) {
				// if the headers of the conflicting edge are equal, only remove this edgej
				toRemove.add(edge);
				
			} else { 
			
				// otherwise remove all correspondences with the same combination of column headers
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
					if(cor.getFirstRecord().getHeader()!=null && cor.getSecondRecord().getHeader()!=null) {
					
						if(cor.getFirstRecord().getHeader().equals(h1) && cor.getSecondRecord().getHeader().equals(h2)
								|| cor.getFirstRecord().getHeader().equals(h2) && cor.getSecondRecord().getHeader().equals(h1)) {
							toRemove.add(cor);
						}
					
					}
				}
			
			}
			
			for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : toRemove) {
				schemaCorrespondences.remove(cor);
				g.removeEdge(cor);
				sb.append(String.format("[conflict]\tRemoved %s<->%s\n", cor.getFirstRecord(), cor.getSecondRecord()));
			}
		}
		
	}
	
	protected String formatPath(List<Correspondence<MatchableTableColumn, MatchableTableRow>> path, BetweennessCentrality<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> between) {	
		StringBuilder sb = new StringBuilder();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> edge : path) {
			sb.append(String.format("%s<-%.4f->%s\t", edge.getFirstRecord(), between.getEdgeRankScore(edge), edge.getSecondRecord()));
		}
		
		return sb.toString();
	}
	
	protected static class Conflict implements Comparable<Conflict> {
		public String header1;
		public String header2;
		public double value;
		
//		public Conflict(String header1, String header2, int count) {
//			this.header1 = header1;
//			this.header2 = header2;
//			this.count = count;
//		}

		public Conflict(String header1, String header2, double value) {
			this.header1 = header1;
			this.header2 = header2;
			this.value = value;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(Conflict o) {
//			return Integer.compare(count, o.count);
			return Double.compare(value, o.value);
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
				
//				if(dh.getDisjointHeaders(header1).contains(header2)) {
				// do not create a conflict if both headers are equal
				if(header1!=null && header2!=null && !header1.equals(header2) && dh.getDisjointHeaders(header1).contains(header2)) {
					Conflict c = new Conflict(header1, header2, groups.get(header1).size() * groups.get(header2).size());
					conflicts.add(c);
				}
			}
		}
		
		Conflict c = conflicts.poll();
		
//		conflicts.addAll(findConflictByTable(cluster));
//		
//		return conflicts;
		
		if(c==null) {
			return findConflictByTable(cluster);
		} else {
			return new Pair<MatchableTableColumn, MatchableTableColumn>(Q.firstOrDefault(groups.get(c.header1)), Q.firstOrDefault(groups.get(c.header2)));
		}
	}
	
	protected Pair<MatchableTableColumn, MatchableTableColumn> findConflictByTable(Collection<MatchableTableColumn> cluster) {
		Map<Integer, MatchableTableColumn> tableToColumn = new HashMap<>();
		
//		Collection<Pair<MatchableTableColumn, MatchableTableColumn>> results = new LinkedList<>();
		
		for(MatchableTableColumn c : cluster) {
			
			MatchableTableColumn conflict = tableToColumn.get(c.getTableId());
			
			if(conflict==null) {
				tableToColumn.put(c.getTableId(), c);
			} else {
				return new Pair<MatchableTableColumn, MatchableTableColumn>(conflict, c);
//				results.add(new Pair<MatchableTableColumn, MatchableTableColumn>(conflict, c));
			}
			
		}
		
		return null;
//		return results;
	}
}

