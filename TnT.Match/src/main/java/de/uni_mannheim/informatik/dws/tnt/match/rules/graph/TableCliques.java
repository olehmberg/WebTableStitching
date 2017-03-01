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
import java.util.Map;
import java.util.Set;

//import org.jgrapht.Graph;
//import org.jgrapht.alg.BronKerboschCliqueFinder;
//import org.jgrapht.graph.DefaultEdge;
//import org.jgrapht.graph.SimpleGraph;

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

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableCliques {

//	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> match(
//			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, 
//			DataProcessingEngine proc,
//			final DisjointHeaders dh) {
//		
//		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
//		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
//			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
//		}
//		
//		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clusters = clusterer.createResult();
//		
//		ResultSet<Pair<MatchableTableColumn, Integer>> clustering = new ResultSet<>();
//		int cluIdx = 0;
//		for(Collection<MatchableTableColumn> cluster : clusters.keySet()) {
//			for(MatchableTableColumn c : cluster) {
//				clustering.add(new Pair<MatchableTableColumn, Integer>(c, cluIdx));
//			}
//			cluIdx++;
//		}
//		
//		Function<MatchableTableColumn, Correspondence<MatchableTableColumn, MatchableTableRow>> joinByLeftColumn = new Function<MatchableTableColumn, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public MatchableTableColumn execute(Correspondence<MatchableTableColumn, MatchableTableRow> input) {
//				return input.getFirstRecord();
//			}
//		};
//		Function<MatchableTableColumn, Pair<MatchableTableColumn, Integer>> joinByClusteredColumn = new Function<MatchableTableColumn, Pair<MatchableTableColumn,Integer>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public MatchableTableColumn execute(Pair<MatchableTableColumn, Integer> input) {
//				return input.getFirst();
//			}
//		};
//		ResultSet<Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<MatchableTableColumn, Integer>>> corsWithClusters = proc.joinMixedTypes(schemaCorrespondences, clustering, joinByLeftColumn, joinByClusteredColumn);
//		
//		RecordKeyValueMapper<Integer, Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<MatchableTableColumn, Integer>>, Correspondence<MatchableTableColumn, MatchableTableRow>> groupByCluster = new RecordKeyValueMapper<Integer, Pair<Correspondence<MatchableTableColumn,MatchableTableRow>,Pair<MatchableTableColumn,Integer>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(
//					Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<MatchableTableColumn, Integer>> record,
//					DatasetIterator<Pair<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {
//
//				resultCollector.next(new Pair<Integer, Correspondence<MatchableTableColumn,MatchableTableRow>>(record.getSecond().getSecond(), record.getFirst()));
//				
//			}
//		};
//		ResultSet<Group<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>>> groupedByCluster = proc.groupRecords(corsWithClusters, groupByCluster);
//		
//		RecordMapper<Group<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn, MatchableTableRow>> resolveConflictsTransformation = new RecordMapper<Group<Integer,Correspondence<MatchableTableColumn,MatchableTableRow>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Group<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
//					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
//
//				Graph<Integer, DefaultEdge> g = new SimpleGraph<>(DefaultEdge.class);
//				
//				
//				ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = record.getRecords();
//				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
//					g.addEdge(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId());
//				}
//				
//				BronKerboschCliqueFinder<Integer, DefaultEdge> clique = new BronKerboschCliqueFinder<>(g);
//				
//				Collection<Set<Integer>> cliques = clique.getAllMaximalCliques();
//				
//				
//				
//				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
//					resultCollector.next(cor);
//				}	
//			}
//		};
//		
//		return proc.transform(groupedByCluster, resolveConflictsTransformation);
//	}
	
}
