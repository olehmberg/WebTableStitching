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
package de.uni_mannheim.informatik.dws.tnt.match;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.SchemaSynonymsClusterer;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.dws.winter.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.dws.winter.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Graph;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Partitioning;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MappingFormatter {


	
	public static void logIdentityResolutionResult(
			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences,
			boolean showFullMatch) {
		System.out.println("Instance Correspondences");
		Map<MatchableTableRow, Collection<Correspondence<MatchableTableRow, MatchableTableColumn>>> candidates = Q
				.group(correspondences.get(),
						new Func<MatchableTableRow, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

							@Override
							public MatchableTableRow invoke(
									Correspondence<MatchableTableRow, MatchableTableColumn> in) {
								return in.getFirstRecord();
							}
						});

		for (MatchableTableRow instance : candidates.keySet()) {
			// System.out.println(instance.format(15));
			System.out.println(StringUtils.join(instance.getValues(), "|"));
			for (Correspondence<MatchableTableRow, MatchableTableColumn> correspondence : candidates
					.get(instance)) {
				
				if(showFullMatch) {
					System.out.println(String.format("\t%s", StringUtils.join(correspondence.getSecondRecord().getValues(), "|")));
				} else {
					System.out.println(String.format("\t%.2f\t[%s]", correspondence
							.getSimilarityScore(), correspondence.getSecondRecord()
							.get(1)));	
				}
			}
		}
	}
	
//	public void writeKeyInstanceGraph(WebTables web, Collection<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences, File file) throws IOException {
//		
//		Map<Set<Integer>, Collection<Correspondence<MatchableTableRow, MatchableTableKey>>> grouped = Q.group(correspondences, new Func<Set<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>>() {
//
//			@Override
//			public Set<Integer> invoke(Correspondence<MatchableTableRow, MatchableTableKey> in) {
//				return Q.toSet(in.getFirstRecord().getTableId(), in.getSecondRecord().getTableId());
//			}});
//		
//		WebTablesMatchingGraph graph = new WebTablesMatchingGraph(web.getTables().values());
//		
//		for(Set<Integer> group : grouped.keySet()) {
//			List<Integer> sorted = Q.sort(group);
//			Table t1 = web.getTables().get(sorted.get(0));
//			Table t2 = web.getTables().get(sorted.get(1));
//			
//			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceMapping = new Processable<>();
//			
//			for(Correspondence<MatchableTableRow, MatchableTableKey> cor : grouped.get(group)) {
//				Correspondence<MatchableTableRow, MatchableTableColumn> newCor = new Correspondence<MatchableTableRow, MatchableTableColumn>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), null);
//				instanceMapping.add(newCor);
//			}
//
//			
//			graph.addInstanceCorrespondence(t1, t2, instanceMapping);
//		}
//		
//		graph.writeInstanceCorrespondenceGraph(file);
//	}
	
//	public void writeInstanceGraph(WebTables web, Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences, File file) throws IOException {
//		
//		Map<Set<Integer>, Collection<Correspondence<MatchableTableRow, MatchableTableColumn>>> grouped = Q.group(correspondences, new Func<Set<Integer>, Correspondence<MatchableTableRow, MatchableTableColumn>>() {
//
//			@Override
//			public Set<Integer> invoke(Correspondence<MatchableTableRow, MatchableTableColumn> in) {
//				return Q.toSet(in.getFirstRecord().getTableId(), in.getSecondRecord().getTableId());
//			}});
//		
//		WebTablesMatchingGraph graph = new WebTablesMatchingGraph(web.getTables().values());
//		
//		for(Set<Integer> group : grouped.keySet()) {
//			List<Integer> sorted = Q.sort(group);
//			Table t1 = web.getTables().get(sorted.get(0));
//			Table t2 = web.getTables().get(sorted.get(1));
//			
//			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceMapping = new Processable<>(grouped.get(group));
//			
//			graph.addInstanceCorrespondence(t1, t2, instanceMapping);
//		}
//		
//		graph.writeInstanceCorrespondenceGraph(file);
//	}
	
//	public void writeKeyGraph(WebTables web, Collection<Correspondence<MatchableTableKey, MatchableTableColumn>> correspondences, File file) throws IOException {
//		
//		Map<Set<Integer>, Collection<Correspondence<MatchableTableKey, MatchableTableColumn>>> grouped = Q.group(correspondences, new Func<Set<Integer>, Correspondence<MatchableTableKey, MatchableTableColumn>>() {
//
//			@Override
//			public Set<Integer> invoke(Correspondence<MatchableTableKey, MatchableTableColumn> in) {
//				return Q.toSet(in.getFirstRecord().getTableId(), in.getSecondRecord().getTableId());
//			}});
//		
//		WebTablesMatchingGraph graph = new WebTablesMatchingGraph(web.getTables().values());
//		
//		for(Set<Integer> group : grouped.keySet()) {
//			List<Integer> sorted = Q.sort(group);
//			Table t1 = web.getTables().get(sorted.get(0));
//			Table t2 = web.getTables().get(sorted.get(1));
//			
//			Set<WebTableMatchingKey> keys = new HashSet<>();
//			
//			for(Correspondence<MatchableTableKey, MatchableTableColumn> cor : grouped.get(group)) {
//				Set<TableColumn> col1 = new HashSet<>();
//				for(MatchableTableColumn mc : cor.getFirstRecord().getColumns()) {
//					col1.add(web.getTables().get(mc.getTableId()).getSchema().get(mc.getColumnIndex()));
//				}
//				Set<TableColumn> col2 = new HashSet<>();
//				for(MatchableTableColumn mc : cor.getSecondRecord().getColumns()) {
//					col2.add(web.getTables().get(mc.getTableId()).getSchema().get(mc.getColumnIndex()));
//				}
//				
//				WebTableMatchingKey k = new WebTableMatchingKey(col1, col2);
//				
//				keys.add(k);
//			}
//			
//			graph.addMatchingKeys(t1, t2, keys);
//		}
//		
//		graph.writeMatchingKeyGraph(file);
//	}
	
	public void writeSchemaCorrespondenceGraph(File f, DataSet<MatchableTableColumn, MatchableTableColumn> allAttributes, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, N2NGoldStandard gs) throws IOException {
		Graph<MatchableTableColumn, Object> g = new Graph<>();

		Map<String, Integer> gsPartition = new HashMap<>();
		int gsIdx = 1;
		if(gs!=null) {
			for(Set<String> clu : gs.getCorrespondenceClusters().keySet()) {
				
				for(String s : clu) {
					gsPartition.put(s, gsIdx);
				}
				
				gsIdx++;
			}
		}
		
		Partitioning<MatchableTableColumn> gsPartitioning = new Partitioning<>(g);
		
		for(MatchableTableColumn c : allAttributes.get()) {
			if(!SpecialColumns.isSpecialColumn(c)) {
				g.addNode(c);
				if(gsPartition.containsKey(c.getIdentifier())) {
					gsPartitioning.setPartition(c, gsPartition.get(c.getIdentifier()));
				} else {
					gsPartitioning.setPartition(c, 0);
				}
			}
		}
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			g.addEdge(cor.getFirstRecord(), cor.getSecondRecord(), cor, cor.getSimilarityScore());
		}
	
		g.writePajekFormat(f);
		gsPartitioning.writePajekFormat(new File(f.getAbsolutePath() + ".gold.clu"));
	}
	
	public void writeSchemaCorrespondenceTableGraph(File f, DataSet<MatchableTableColumn, MatchableTableColumn> allAttributes, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) throws IOException {
		Graph<Integer, Object> g = new Graph<>();

		for(MatchableTableColumn c : allAttributes.get()) {
			if(!SpecialColumns.isSpecialColumn(c)) {
				g.addNode(c.getTableId());
			}
		}
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			g.addEdge(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId(), cor, cor.getSimilarityScore());
		}
	
		g.writePajekFormat(f);
	}
	
	public void printKeys(Collection<MatchableTableDeterminant> keys) {
		Map<Integer, Collection<MatchableTableDeterminant>> grouped = Q.group(keys, new MatchableTableDeterminant.TableIdProjection());
		
		List<Integer> tables = Q.sort(grouped.keySet());
		
		for(Integer tableId : tables) {
			Collection<MatchableTableDeterminant> tableKeys = grouped.get(tableId);
			
			System.out.println(String.format("#%d", tableId));
			
			for(MatchableTableDeterminant k : tableKeys) {
				System.out.println(String.format("\t%s", k.getColumns()));
			}
		}
	}
	

	public void printSchemaSynonyms(DataSet<MatchableTableColumn, MatchableTableColumn> allColumns, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {	
    	// add schema correspondences via attribute names
    	SchemaSynonymsClusterer synonymClusterer = new SchemaSynonymsClusterer();
    	Processable<Set<String>> synonyms = synonymClusterer.runClustering(allColumns, true, Correspondence.toMatchable(schemaCorrespondences));
    	System.out.println("Synonyms");
    	for(Set<String> clu : synonyms.get()) {
    		System.out.println("\t" + StringUtils.join(clu, ","));
    	}
	}

	public void printSchemaCorrespondences(Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences) {
		
		Map<String, Collection<Correspondence<MatchableTableColumn, MatchableTableRow>>> grouped = Q.group(correspondences.get(), new Func<String, Correspondence<MatchableTableColumn, MatchableTableRow>>() {

			@Override
			public String invoke(Correspondence<MatchableTableColumn, MatchableTableRow> in) {
				return "#" + in.getFirstRecord().getTableId() + "<-> #" + in.getSecondRecord().getTableId();
			}
		});
		
		for(String group : grouped.keySet()) {
			
			System.out.println(group);
			
			SimilarityMatrix<MatchableTableColumn> m = SimilarityMatrix.fromCorrespondences(grouped.get(group), new SparseSimilarityMatrixFactory());
			System.out.println(m.getOutput());
//			for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : grouped.get(group)) {
//				System.out.println(String.format("\t%s<->%s", cor.getFirstRecord(), cor.getSecondRecord()));
//			}
			
		}
	}

	public void printKeyInstanceCorrespondences(Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> correspondences, int id1, int id2) {
		RecordKeyValueMapper<String, Correspondence<MatchableTableRow, MatchableTableDeterminant>, Correspondence<MatchableTableRow, MatchableTableDeterminant>> groupByTableCombination = new RecordKeyValueMapper<String, Correspondence<MatchableTableRow,MatchableTableDeterminant>, Correspondence<MatchableTableRow,MatchableTableDeterminant>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
		
			@Override
			public void mapRecordToKey(Correspondence<MatchableTableRow, MatchableTableDeterminant> record,
					DataIterator<Pair<String, Correspondence<MatchableTableRow, MatchableTableDeterminant>>> resultCollector) {
				
				String key = record.getFirstRecord().getTableId() + "/" + record.getSecondRecord().getTableId();
				
				resultCollector.next(new Pair<String, Correspondence<MatchableTableRow,MatchableTableDeterminant>>(key, record));
				
			}
		};
		
		
		Processable<Group<String, Correspondence<MatchableTableRow, MatchableTableDeterminant>>> byTable = correspondences.group(groupByTableCombination);
		
		for(Group<String, Correspondence<MatchableTableRow, MatchableTableDeterminant>> group : byTable.get()) {
			
			Correspondence<MatchableTableRow, MatchableTableDeterminant> firstCor = Q.firstOrDefault(group.getRecords().get());
			
			int t1 = firstCor.getFirstRecord().getTableId();
			int t2 = firstCor.getSecondRecord().getTableId();
			
			if( (id1==-1||t1==id1) && (id2==-1||t2==id2)) {
			
				System.out.println(String.format("Tables %d<->%d", t1, t2));
				for(Correspondence<MatchableTableRow, MatchableTableDeterminant> cor : group.getRecords().get()) {
					System.out.println(String.format("\t[#%d]\t%s", cor.getFirstRecord().getRowNumber(), cor.getFirstRecord().format(20)));
					System.out.println(String.format("\t[#%d]\t%s", cor.getSecondRecord().getRowNumber(), cor.getSecondRecord().format(20)));
				}
			
			}
		}
	}
	
	public void printInstanceCorrespondences(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences, int id1, int id2) {
		RecordKeyValueMapper<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>> groupByTableCombination = new RecordKeyValueMapper<String, Correspondence<MatchableTableRow,MatchableTableColumn>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
		
			@Override
			public void mapRecordToKey(Correspondence<MatchableTableRow, MatchableTableColumn> record,
					DataIterator<Pair<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {
				
				String key = record.getFirstRecord().getTableId() + "/" + record.getSecondRecord().getTableId();
				
				resultCollector.next(new Pair<String, Correspondence<MatchableTableRow,MatchableTableColumn>>(key, record));
				
			}
		};
		
		
		Processable<Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> byTable = correspondences.group(groupByTableCombination);
		
		for(Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>> group : byTable.get()) {
			
			Correspondence<MatchableTableRow, MatchableTableColumn> firstCor = Q.firstOrDefault(group.getRecords().get());
			
			int t1 = firstCor.getFirstRecord().getTableId();
			int t2 = firstCor.getSecondRecord().getTableId();
			
			if( (id1==-1||t1==id1) && (id2==-1||t2==id2)) {
			
				System.out.println(String.format("Tables %d<->%d", t1, t2));
				for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : group.getRecords().get()) {
					System.out.println(String.format("\t[#%d]\t%s", cor.getFirstRecord().getRowNumber(), cor.getFirstRecord().format(20)));
					System.out.println(String.format("\t[#%d]\t%s", cor.getSecondRecord().getRowNumber(), cor.getSecondRecord().format(20)));
				}
			
			}
		}
	}
	
	public void printKeyCorrespondences(Collection<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> keyCorrespondences) {
		
		List<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> sorted = Q.sort(keyCorrespondences, new Comparator<Correspondence<MatchableTableDeterminant, MatchableTableColumn>>() {

			@Override
			public int compare(Correspondence<MatchableTableDeterminant, MatchableTableColumn> o1,
					Correspondence<MatchableTableDeterminant, MatchableTableColumn> o2) {
				int result = Integer.compare(o1.getFirstRecord().getTableId(), o2.getFirstRecord().getTableId());
				
				if(result!=0) {
					return result;
				}
				
				result = Integer.compare(o1.getSecondRecord().getTableId(), o2.getSecondRecord().getTableId());
				
				if(result!=0) {
					return result;
				}
				
				return Integer.compare(o1.getFirstRecord().getColumns().size(), o2.getFirstRecord().getColumns().size());
			}
		});
		
		for(Correspondence<MatchableTableDeterminant, MatchableTableColumn> key : sorted) {
			System.out.println(String.format("{#%d}{%s} <-> {#%d}{%s}", 
					key.getFirstRecord().getTableId(), 
					Q.project(key.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()),
					key.getSecondRecord().getTableId(), 
					Q.project(key.getSecondRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection())));
		}
		
	}
	
	public void printVotesForKeys(Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> correspondences,
			final int id1, final int id2) {
		
		RecordKeyValueMapper<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableDeterminant>, Correspondence<MatchableTableRow, MatchableTableDeterminant>> groupByTablesMapper = new RecordKeyValueMapper<Collection<Integer>, Correspondence<MatchableTableRow,MatchableTableDeterminant>, Correspondence<MatchableTableRow,MatchableTableDeterminant>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(Correspondence<MatchableTableRow, MatchableTableDeterminant> record,
					DataIterator<Pair<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableDeterminant>>> resultCollector) {
				
				if(record.getFirstRecord().getTableId()>record.getSecondRecord().getTableId()) {
					System.out.println("Wrong direction!");
				}
				
				if( (id1==-1||record.getFirstRecord().getTableId()==id1) && (id2==-1||record.getSecondRecord().getTableId()==id2)) {
					resultCollector.next(new Pair<Collection<Integer>, Correspondence<MatchableTableRow,MatchableTableDeterminant>>(Q.toList(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				}
			}
		};
		
		// group by table
		Processable<Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableDeterminant>>> grouped = correspondences.group(groupByTablesMapper);
		
		RecordMapper<Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableDeterminant>>, Correspondence<MatchableTableRow, MatchableTableDeterminant>> removeUntrustedCorrespondences = new RecordMapper<Group<Collection<Integer>,Correspondence<MatchableTableRow,MatchableTableDeterminant>>, Correspondence<MatchableTableRow,MatchableTableDeterminant>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableDeterminant>> record,
					DataIterator<Correspondence<MatchableTableRow, MatchableTableDeterminant>> resultCollector) {

				System.out.println(String.format("*** %s ***", record.getKey()));
				
				// combine all correspondences between the same records (with different keys)
				HashMap<List<Integer>, Correspondence<MatchableTableRow, MatchableTableDeterminant>> correspondences = new HashMap<>();
				HashMap<List<Integer>, Processable<Correspondence<MatchableTableDeterminant, MatchableTableRow>>> causalCorrespondences = new HashMap<>();
				
				// count the key combinations in the correspondences
				SimilarityMatrix<MatchableTableDeterminant> keyFrequencies = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
				for(Correspondence<MatchableTableRow, MatchableTableDeterminant> cor : record.getRecords().get()) {
					List<Integer> rows = Q.toList(cor.getFirstRecord().getRowNumber(), cor.getSecondRecord().getRowNumber());
					Correspondence<MatchableTableRow, MatchableTableDeterminant> merged = correspondences.get(rows);
					Processable<Correspondence<MatchableTableDeterminant, MatchableTableRow>> causes = causalCorrespondences.get(rows);
					if(merged==null) {
						causes = new ProcessableCollection<Correspondence<MatchableTableDeterminant, MatchableTableRow>>();
						merged = new Correspondence<MatchableTableRow, MatchableTableDeterminant>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), Correspondence.toMatchable(causes));
						correspondences.put(rows, merged);
						causalCorrespondences.put(rows, causes);
					}
					
					for(Correspondence<MatchableTableDeterminant, Matchable> keyCor : cor.getCausalCorrespondences().get()) {
					
						//merged.getCausalCorrespondences().add(keyCor);
						Correspondence<MatchableTableDeterminant, MatchableTableRow> castCor = new Correspondence<MatchableTableDeterminant, MatchableTableRow>(keyCor.getFirstRecord(), keyCor.getSecondRecord(), keyCor.getSimilarityScore(), null);
						causes.add(castCor);
						
						MatchableTableDeterminant k1 = keyCor.getFirstRecord();
						MatchableTableDeterminant k2 = keyCor.getSecondRecord();
						
						Double existing = keyFrequencies.get(k1, k2);
						if(existing==null) {
							existing=0.0;
						}
						
						keyFrequencies.set(k1, k2, existing+1.0);
					}
				}
				
				System.out.println(keyFrequencies.getOutput());
				
				
				// decide for the most frequent combinations
				BestChoiceMatching bcm = new BestChoiceMatching();
				SimilarityMatrix<MatchableTableDeterminant> trustedKeys = bcm.match(keyFrequencies);
				
				Correspondence<MatchableTableRow, MatchableTableDeterminant> anyCor = Q.firstOrDefault(correspondences.values());
				System.out.println(String.format("Table #%d: %d candidate keys", anyCor.getFirstRecord().getTableId(), anyCor.getFirstRecord().getKeys().length));
				for(MatchableTableColumn[] key : anyCor.getFirstRecord().getKeys()) {
					System.out.println(String.format("\t%s", Arrays.asList(key)));
				}
				System.out.println(String.format("Table #%d: %d candidate keys", anyCor.getSecondRecord().getTableId(), anyCor.getSecondRecord().getKeys().length));
				for(MatchableTableColumn[] key : anyCor.getSecondRecord().getKeys()) {
					System.out.println(String.format("\t%s", Arrays.asList(key)));
				}
				
				trustedKeys.normalize();
				trustedKeys.prune(1.0);
				
				System.out.println(trustedKeys.getOutput());
				
				for(MatchableTableDeterminant k1 : trustedKeys.getFirstDimension()) {
					for(MatchableTableDeterminant k2 : trustedKeys.getMatches(k1)) {
						System.out.println(String.format("%s<->%s", k1, k2));
					}
				}
				
				// only output correspondences where all the frequent combinations matched
				for(Correspondence<MatchableTableRow, MatchableTableDeterminant> cor : correspondences.values()) {
					
					// check all key correspondences
					Iterator<Correspondence<MatchableTableDeterminant, Matchable>> keyIt = cor.getCausalCorrespondences().get().iterator();
					while(keyIt.hasNext()) {
						Correspondence<MatchableTableDeterminant, Matchable> keyCor = keyIt.next();
						
						if(trustedKeys.get(keyCor.getFirstRecord(), keyCor.getSecondRecord())==null) {
							// this is an untrusted key and hence removed
							keyIt.remove();
						}
					}
					
					// now only trusted key correspondences remain
					// if the number of key correspondences does not match the number of trusted keys, the instance correspondence is untrusted and removed
					if(cor.getCausalCorrespondences().size()==trustedKeys.getNumberOfNonZeroElements()) {
						System.out.println(String.format("++ %s", cor.getFirstRecord().format(20)));
						System.out.println(String.format("++ %s", cor.getSecondRecord().format(20)));
					} else {
						
//						System.out.println(keyFrequencies.getOutput());
//						System.out.println(trustedKeys.getOutput());
//						
//						for(MatchableTableKey k1 : trustedKeys.getFirstDimension()) {
//							for(MatchableTableKey k2 : trustedKeys.getMatches(k1)) {
//								
//								boolean found = false;
//								for(Correspondence<MatchableTableKey, MatchableTableRow> keyCor : cor.getCausalCorrespondences().get()) {
//									if(keyCor.getFirstRecord().equals(k1) && keyCor.getSecondRecord().equals(k2)) {
//										found=true;
//										break;
//									}
//								}
//								
//								if(!found) {
//									System.out.println(String.format("-x {%s}<->{%s}", k1.getColumns(), k2.getColumns()));
//								}
//								
//							}
//						}
//						
						System.out.println(String.format("-- %s", cor.getFirstRecord().format(20)));
						System.out.println(String.format("-- %s", cor.getSecondRecord().format(20)));
//						
//						System.out.println();
					}
					
				}
			}
		};
		
		grouped.map(removeUntrustedCorrespondences);
		
	}

	public void printInstanceMappingDetails(WebTables web, N2NGoldStandard mapping) {
		
		for(Set<String> cluster : mapping.getCorrespondenceClusters().keySet()) {
			
			System.out.println(mapping.getCorrespondenceClusters().get(cluster));
			for(String record : cluster) {
				MatchableTableRow row = web.getRecords().getRecord(record);
				System.out.println(String.format("%s\t%s", row.getIdentifier(), row.format(30)));
			}
			
		}
		
	}
	
	public void printVotesForSchemaCorrespondences(Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, final int id1, final int id2) {
		
		Map<String, Collection<Correspondence<MatchableTableRow, MatchableTableColumn>>> grouped = Q.group(instanceCorrespondences, new Func<String, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

			@Override
			public String invoke(Correspondence<MatchableTableRow, MatchableTableColumn> in) {
				if ( (id1==-1||id1==in.getFirstRecord().getTableId()) && (id2==-1||id2==in.getSecondRecord().getTableId())) {
					return "#" + in.getFirstRecord().getTableId() + "<->" +in.getSecondRecord().getTableId();
				} else {
					return "";
				}
			}});
		
		for(String group : grouped.keySet()) {
			if(!group.equals("")) {
				
				System.out.println(group);
				
				Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> votes = new LinkedList<>();
				for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : grouped.get(group)) {
					System.out.println(String.format("\t%s", cor.getFirstRecord().format(20)));
					System.out.println(String.format("\t%s", cor.getSecondRecord().format(20)));
					votes.addAll(cor.<MatchableTableRow>castCausalCorrespondences().get());
				}
				
				SimilarityMatrix<MatchableTableColumn> m = SimilarityMatrix.fromCorrespondences(votes, new SparseSimilarityMatrixFactory());
				
				System.out.println(m.getOutput());
				
			}
		}
	}
	
	public void printAttributeClusters(Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		ConnectedComponentClusterer<String> clusterer = new ConnectedComponentClusterer<>();
		Set<String> attributes = new HashSet<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<String, String, Double>(cor.getFirstRecord().getHeader(), cor.getSecondRecord().getHeader(), cor.getSimilarityScore()));
			attributes.add(cor.getFirstRecord().getHeader());
			attributes.add(cor.getSecondRecord().getHeader());
		}
		for(String att : attributes) {
			clusterer.addEdge(new Triple<String, String, Double>(att, att, 1.0));
		}
		
		for(Collection<String> clu :  clusterer.createResult().keySet()) {
			if(clu.size()>1) {
				System.out.println(String.format("\t%s", StringUtils.join(clu, ",")));
			}
		}
	}
}
