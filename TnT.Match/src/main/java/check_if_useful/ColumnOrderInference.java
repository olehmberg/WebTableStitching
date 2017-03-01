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
package check_if_useful;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.mining.SequentialPatternMiner;
import de.uni_mannheim.informatik.dws.tnt.mining.SequentialPatternMiner.Sequence;
import de.uni_mannheim.informatik.dws.tnt.mining.SequentialPatternMiner.SequentialRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ColumnOrderInference {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> inferCorrespondencesByColumnOrder(Collection<Table> tables, Set<Collection<TableColumn>> attributeClusters) {

		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> newCorrespondences = new ResultSet<>();
		
		int cluIdx = 0;
//		for(Collection<Table> tables : getConnectedTables()) {
			
			System.out.println(String.format("Frequent Column Positions for Cluster %d", cluIdx));
			
//			Map<Set<FrequentColumnPosition>, Integer> itemSets = calculateFrequentItemSetsOfColumnPositions(allTables);
			
//			System.out.println(formatFrequentItemSets(itemSets));
			
//			Map<Set<FrequentColumnPosition>, Set<FrequentColumnPosition>> rules = calculateAssociationRulesForColumnPositions(itemSets);
			
//			System.out.println(formatAssociationRules(rules, itemSets));
			
			Set<Sequence<Collection<TableColumn>>> sequentialPatterns = calculateSequentialPatternsOfColumnPositions(tables, attributeClusters);
			
			System.out.println(formatSequentialPatterns(sequentialPatterns));
			
			SequentialPatternMiner<Collection<TableColumn>> seqMiner = new SequentialPatternMiner<Collection<TableColumn>>();
			Set<SequentialRule<Collection<TableColumn>>> sequentialRules = seqMiner.calculateSequentialRules(sequentialPatterns);
//			List<Pair<List<FrequentColumnPosition>, List<FrequentColumnPosition>>> sequentialRules = calculateSequentialRulesForColumnPositions(sequentialPatterns);
			
			System.out.println(formatSequentialRules(sequentialRules));
			
//			TableSchemaStatistics stat = new TableSchemaStatistics();
//			for(Table t : tables) {
//				stat.addTable(t);
//			}
//			stat.print();
			
			// TODO translate the rules with absolute column indices to rules with relative column indices
			// --> sequential pattern mining
			
			// determine all sequential rules that match the table
			// confidence must be lower than one, otherwise we cannot add any correspondences
			// condition must not lead to contradicting consequents!
			for(Table t : tables) {
				
				Set<SequentialRule<Collection<TableColumn>>> applicableRules = new HashSet<>();
				Set<SequentialRule<Collection<TableColumn>>> applicableInvertedRules = new HashSet<>();
				
//				Map<Set<FrequentColumnPosition>, Set<FrequentColumnPosition>> applicableRules = new HashMap<>();
				
				for(SequentialRule<Collection<TableColumn>> rule : sequentialRules) {
					
					if(
//							rule.getConfidence() == 1.0 &&
							rule.getCondition().matches(mapTableToTranslatedTable.get(t))) {
						applicableRules.add(rule);
					}

//					double confidenceInv = (double)rule.getAllElementsSupportCount() / (double)rule.getConsequent().getCount();
					if(
//							confidenceInv == 1.0 &&
							rule.getConsequent().matches(mapTableToTranslatedTable.get(t))) {
						applicableInvertedRules.add(rule);
					}
				}
			
				// add inverse rules as actual rules
				Set<SequentialRule<Collection<TableColumn>>> temp = new HashSet<>();
				for(SequentialRule<Collection<TableColumn>> rule : applicableInvertedRules) {
					SequentialRule<Collection<TableColumn>> inverse = new SequentialRule<>(rule.getConsequent(), rule.getCondition(), rule.getAllElementsSupportCount());
					temp.add(inverse);
				}
				applicableInvertedRules = temp;
				
				// check for conflicts
				checkRulesForConflicts(applicableRules);
				checkRulesForConflicts(applicableInvertedRules);
				
				System.out.println(String.format("%d non-conflicting rules", applicableRules.size()+applicableInvertedRules.size()));
				System.out.println(formatSequentialRulesGroupedByCondition(applicableRules));
				System.out.println(formatSequentialRulesGroupedByCondition(applicableInvertedRules));
				
				
				System.out.println(String.format("%d sequential rules satisfied for table %d", applicableRules.size(), t.getTableId()));
				
				int addCount = 0;
				// apply the rules to generate correspondences
				for(SequentialRule<Collection<TableColumn>> rule : applicableRules) {
					
					int consequentStartIndex = rule.getCondition().getMatchIndex(mapTableToTranslatedTable.get(t)) + rule.getCondition().getSize();
					
					// make sure the table is long enough to match the consequent
					if(t.getSchema().getSize()>consequentStartIndex+rule.getConsequent().getSize()) {
						for(int i=0; i<rule.getConsequent().getSize(); i++) {
							TableColumn affectedColumn = t.getSchema().get(consequentStartIndex+i);
							if(!rule.getConsequent().getElements().get(i).contains(affectedColumn)) {
								MatchableTableColumn first = new MatchableTableColumn(t.getTableId(), affectedColumn);
								
								TableColumn anyOtherColumn = rule.getConsequent().getElements().get(i).iterator().next();
								MatchableTableColumn second = new MatchableTableColumn(anyOtherColumn.getTable().getTableId(), anyOtherColumn);
								
								Correspondence<MatchableTableColumn, MatchableTableRow> newCor = new Correspondence<MatchableTableColumn, MatchableTableRow>(first, second, 1.0, null);
								
	//							addSchemaCorrespondence(t, anyOtherColumn.getTable(), newCor);
								newCorrespondences.add(newCor);
								
								
								System.out.println(String.format("[ColumnPosition] Adding Correspondence %s<->%s", affectedColumn, anyOtherColumn));
								
								addCount++;
							}
						}
					}
				}
				for(SequentialRule<Collection<TableColumn>> rule : applicableInvertedRules) {
					
					int consequentStartIndex = rule.getCondition().getMatchIndex(mapTableToTranslatedTable.get(t)) - rule.getConsequent().getSize();
					
					if(consequentStartIndex>=0) {
						for(int i=0; i<rule.getConsequent().getSize(); i++) {
							TableColumn affectedColumn = t.getSchema().get(consequentStartIndex+i);
							if(!rule.getConsequent().getElements().get(i).contains(affectedColumn)) {
								MatchableTableColumn first = new MatchableTableColumn(t.getTableId(), affectedColumn);
								
								TableColumn anyOtherColumn = rule.getConsequent().getElements().get(i).iterator().next();
								MatchableTableColumn second = new MatchableTableColumn(anyOtherColumn.getTable().getTableId(), anyOtherColumn);
								
								Correspondence<MatchableTableColumn, MatchableTableRow> newCor = new Correspondence<MatchableTableColumn, MatchableTableRow>(first, second, 1.0, null);
								
	//							addSchemaCorrespondence(t, anyOtherColumn.getTable(), newCor);
								newCorrespondences.add(newCor);
								
								
								System.out.println(String.format("[ColumnPosition] Adding Correspondence %s<->%s", affectedColumn, anyOtherColumn));
								
								addCount++;
							}
						}
					}
				}
//				for(SequentialRule<Collection<TableColumn>> rule : applicableInvertedRules) {
//					
//					int conditionStartIndex = rule.getConsequent().getMatchIndex(mapTableToTranslatedTable.get(t)) - rule.getCondition().getSize();
//					
//					for(int i=0; i<rule.getCondition().getSize(); i++) {
//						TableColumn affectedColumn = t.getSchema().get(conditionStartIndex+i);
//						if(!rule.getCondition().getElements().get(i).contains(affectedColumn)) {
//							MatchableTableColumn first = new MatchableTableColumn(t.getTableId(), affectedColumn);
//							
//							TableColumn anyOtherColumn = rule.getCondition().getElements().get(i).iterator().next();
//							MatchableTableColumn second = new MatchableTableColumn(anyOtherColumn.getTable().getTableId(), anyOtherColumn);
//							
//							Correspondence<MatchableTableColumn, MatchableTableRow> newCor = new Correspondence<MatchableTableColumn, MatchableTableRow>(first, second, 1.0, null);
//							
////							addSchemaCorrespondence(t, anyOtherColumn.getTable(), newCor);
//							newCorrespondences.add(newCor);
//							addCount++;
//							
//							System.out.println(String.format("[ColumnPositionInverse] Adding Correspondence %s<->%s", affectedColumn, anyOtherColumn));
//						}
//					}
//				}
//					
//					for(Collection<TableColumn> consequent : rule.getConsequent().getElements()) {
//						
//						TableColumn affectedColumn = t.getSchema().get(consequentColumn.columnIndex);
//						
//							// check if the correspondence already exists
//							if(!consequentColumn.cluster.contains(affectedColumn)) {
//							
//								MatchableTableColumn first = new MatchableTableColumn(t.getTableId(), affectedColumn);
//								
//								TableColumn anyOtherColumn = consequentColumn.cluster.iterator().next();
//								MatchableTableColumn second = new MatchableTableColumn(anyOtherColumn.getTable().getTableId(), anyOtherColumn);
//								
//								Correspondence<MatchableTableColumn, MatchableTableRow> newCor = new Correspondence<MatchableTableColumn, MatchableTableRow>(first, second, 1.0, null);
//								
//								addSchemaCorrespondence(t, anyOtherColumn.getTable(), newCor);
//								addCount++;
//								
//								System.out.println(String.format("[ColumnPosition] Adding Correspondence %s<->%s", affectedColumn, anyOtherColumn));
//						}
//					}
					
//				}
				
				System.out.println(String.format("Added %d correspondences for table %d via column positions", addCount, t.getTableId()));
				
			}

			
//			cluIdx++;
//		}
			
			return newCorrespondences;
		
	}
	
	public void checkRulesForConflicts(Set<SequentialRule<Collection<TableColumn>>> applicableRules) {

		// check rules for conflicts
		Map<Sequence<Collection<TableColumn>>, Collection<SequentialRule<Collection<TableColumn>>>> grouped = Q.group(applicableRules, new Func<Sequence<Collection<TableColumn>>, SequentialRule<Collection<TableColumn>>>() {

			@Override
			public Sequence<Collection<TableColumn>> invoke(SequentialRule<Collection<TableColumn>> in) {
				return in.getCondition();
			}
		});
		
		
		for(Sequence<Collection<TableColumn>> key : grouped.keySet()) {
			Collection<SequentialRule<Collection<TableColumn>>> group = grouped.get(key);
		
			SequentialRule<Collection<TableColumn>> longestRule = null;
			boolean hasConflict = false;
			
			for(SequentialRule<Collection<TableColumn>> rule : group) {
				
				if(longestRule==null) {
					longestRule = rule;
				} else {
					
					boolean longestMatchesCurrent = longestRule.getConsequent().matches(rule.getConsequent().getElements()); 
					boolean currentMatchesLongest = rule.getConsequent().matches(longestRule.getConsequent().getElements()); // current is contained in longest
					
					if(longestMatchesCurrent && currentMatchesLongest) {
						// identical rules
					} else if(longestMatchesCurrent) {
						// longest is contained in current
						longestRule = rule;
					} else if(!longestMatchesCurrent && !currentMatchesLongest) {
						// no match between rules -> conflict!
						hasConflict = true;
						break;
					}
					
				}
			}
			
			applicableRules.removeAll(group);
			
			if(!hasConflict && longestRule!=null) {
				applicableRules.add(longestRule);
			}
		}
		
	}

	public static class FrequentColumnPosition {
		Collection<TableColumn> cluster;
		int columnIndex;
		int tableSize;
		
		public FrequentColumnPosition(Collection<TableColumn> cluster, int columnIndex, int tableSize) {
			this.cluster = cluster;
			this.columnIndex = columnIndex;
			this.tableSize = tableSize;
		}
		
		public boolean matches(Table t) {
			return t.getColumns().size() == tableSize && cluster.contains(t.getSchema().get(columnIndex));
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return (997 * ((int)cluster.hashCode()) ^ 991 * columnIndex) ^ 1099 * tableSize ; 
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof FrequentColumnPosition) {
				FrequentColumnPosition fcp = (FrequentColumnPosition)obj;
				return cluster.equals(fcp.cluster) && columnIndex==fcp.columnIndex && tableSize==fcp.tableSize;
			} else {
				return super.equals(obj);
			}
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			TableColumn first = Q.firstOrDefault(cluster);
			return String.format("(%s;%d;%d)", first, columnIndex, tableSize);
		}
	}
	
	public Map<Set<FrequentColumnPosition>, Integer> calculateFrequentItemSetsOfColumnPositions(Collection<Table> tables, Set<Collection<TableColumn>> attributeClusters) {

		Map<Set<FrequentColumnPosition>, Integer> itemSets = new HashMap<>();
		
//		for(Collection<Table> tables : getConnectedTables()) {
//			Set<Collection<TableColumn>> clusters = getAttributeClusters(new ArrayList<>(tables));
			
			// create all 1-item sets
			for(Collection<TableColumn> cluster : attributeClusters) {
				Distribution<Integer> columnIndexDistribution = Distribution.fromCollection(cluster, new Func<Integer, TableColumn>() {

					@Override
					public Integer invoke(TableColumn in) {
						return in.getColumnIndex();
					}
				});
				
				for(final int i : columnIndexDistribution.getElements()) {
					
					Collection<TableColumn> filtered = Q.where(cluster, new Func<Boolean, TableColumn>() {

						@Override
						public Boolean invoke(TableColumn in) {
							return in.getColumnIndex()==i;
						}
					});
					
					Distribution<Integer> tableSizeDistribution = Distribution.fromCollection(filtered, new Func<Integer, TableColumn>() {

						@Override
						public Integer invoke(TableColumn in) {
							return in.getTable().getColumns().size();
						}
					});
					
					for(int s : tableSizeDistribution.getElements()) {
						FrequentColumnPosition fp = new FrequentColumnPosition(cluster, i, s);
						
						// create the item set
						Set<FrequentColumnPosition> itemset = new HashSet<>();
						itemset.add(fp);
						
						itemSets.put(itemset, tableSizeDistribution.getFrequency(s));
					}

				}
			}
			
			// create all i+1 item sets
			boolean hasChanges = false;
			
			// easy access to item sets generated in the last round
			Set<Set<FrequentColumnPosition>> OneItemSets = new HashSet<>(itemSets.keySet());
			Set<Set<FrequentColumnPosition>> lastItemSets = itemSets.keySet();
			Set<Set<FrequentColumnPosition>> currentItemSets = new HashSet<>();
			
			// loop until no new item sets are discovered
			do {
				
				// iterate over all item sets created in the last round
				for(Set<FrequentColumnPosition> itemset1 : lastItemSets) {
					
					// and combine them with the 1-item sets to create new item sets
					for(Set<FrequentColumnPosition> itemset2 : OneItemSets) { 
						
						if(!itemset1.equals(itemset2) && !itemset1.containsAll(itemset2)) {
						
							Set<FrequentColumnPosition> itemset = new HashSet<>();
							
							itemset.addAll(itemset1);
							itemset.addAll(itemset2);

							currentItemSets.add(itemset);							
						}
						
					}
				}
				

				// calculate frequency of new item sets
				// loop over all table schemas
				for(Table t : tables) {
					
					// if the schema contains columns mapped to all clusters of an itemset, increase the count
					// iterate over all new itemsets
					for(Set<FrequentColumnPosition> itemset : currentItemSets) {
						
						Set<FrequentColumnPosition> items = new HashSet<>(itemset);
						
						// iterate over all items and check if they exist in the data
						for(FrequentColumnPosition cluAndPos : itemset) {
							
							for(TableColumn c : t.getSchema().getRecords()) {
								
								if(c.getTable().getColumns().size()==cluAndPos.tableSize && c.getColumnIndex()==cluAndPos.columnIndex && cluAndPos.cluster.contains(c)) {
									items.remove(cluAndPos);
									break;
								}
								
							}

						}
						
						// if all the items have been found, 'items' must be empty now
						if(items.size()==0) {
							// increment the frequency count
							MapUtils.increment(itemSets, itemset);
						}
					}
					
				}
				
				// filter the itemsets
				Iterator<Set<FrequentColumnPosition>> it = currentItemSets.iterator();
				while(it.hasNext()) {
					Set<FrequentColumnPosition> itemset = it.next();
					
					Integer frequency = itemSets.get(itemset);
					
					if(frequency==null || frequency<1) {
						it.remove();
						itemSets.remove(itemset);
					}
				}
				
				hasChanges = currentItemSets.size()>0;
				lastItemSets = currentItemSets;
				currentItemSets = new HashSet<>();
				
			} while(hasChanges);
//		}


		
		return itemSets;
	}
	
	public String formatFrequentItemSets(Map<Set<FrequentColumnPosition>, Integer> itemSets) {

		StringBuilder sb = new StringBuilder();
		
		// print results
		List<Set<FrequentColumnPosition>> sorted = Q.sort(itemSets.keySet(), new Comparator<Set<FrequentColumnPosition>>() {

			@Override
			public int compare(Set<FrequentColumnPosition> o1,
					Set<FrequentColumnPosition> o2) {
				return Integer.compare(o1.size(), o2.size());
			}
		});
		
		sb.append("Frequent Item Sets:\n");
		sb.append("c\tf\titems\n");
		for(Set<FrequentColumnPosition> itemset : sorted) {
			sb.append(String.format("%d\t%d\t{%s}\n", itemset.size(), itemSets.get(itemset), StringUtils.join(Q.project(itemset, new Func<String, FrequentColumnPosition>() {

				@Override
				public String invoke(FrequentColumnPosition in) {
					TableColumn col = Q.firstOrDefault(in.cluster);
					
					return String.format("(%s;%d;%d)", col.getHeader(), in.columnIndex, in.tableSize);
				}
			}), ",")));
		}
		
		return sb.toString();
	}
	
	public Map<Set<FrequentColumnPosition>, Set<FrequentColumnPosition>> calculateAssociationRulesForColumnPositions(Map<Set<FrequentColumnPosition>, Integer> itemSets) {

		// for each attribute cluster, get the distribution of column indices
		// do the same for all combinations of column clusters
		// frequent itemset mining, minimum confidence = 1, minimum support count > 1
	
		
		// create association rules
		final Map<Set<FrequentColumnPosition>, Set<FrequentColumnPosition>> rules = new HashMap<>();
		
		// iterate over all frequent item sets
		for(Set<FrequentColumnPosition> itemset : itemSets.keySet()) {
			if(itemset.size()>1) {
				
				// move each item from the condition to the consequent, step by step
				for(FrequentColumnPosition item : itemset) {
					Set<FrequentColumnPosition> condition = new HashSet<>(itemset);
					condition.remove(item);
					Set<FrequentColumnPosition> consequent = new HashSet<>();
					consequent.add(item);
					
//					double confidence = (double)itemSets.get(itemset) / (double)itemSets.get(condition);
					
//					if(confidence==1.0) {
						rules.put(condition, consequent);
//					}
				}
				
			}
		}
		
		return rules;
	}
	
	public String formatAssociationRules(final Map<Set<FrequentColumnPosition>, Set<FrequentColumnPosition>> rules, Map<Set<FrequentColumnPosition>, Integer> itemSets) {
		StringBuilder sb = new StringBuilder();
		
		// print rules
		Collection<Set<FrequentColumnPosition>> sortedRules = Q.sort(rules.keySet(), new Comparator<Set<FrequentColumnPosition>>() {

			@Override
			public int compare(Set<FrequentColumnPosition> o1, Set<FrequentColumnPosition> o2) {
				return Integer.compare(rules.get(o1).size(), rules.get(o2).size());
			}
			
		});

		sb.append("Association rules:\n");
		for(Set<FrequentColumnPosition> condition : sortedRules) {
			
			Set<FrequentColumnPosition> consequent = rules.get(condition);
			
			double confidence = (double)itemSets.get(Q.union(condition, consequent)) / (double)itemSets.get(condition);
			
			sb.append(String.format("[%.4f] {%s}->{%s}\n",
					confidence,
					StringUtils.join(Q.project(condition, new Func<String, FrequentColumnPosition>() {

						@Override
						public String invoke(FrequentColumnPosition in) {
							TableColumn col = Q.firstOrDefault(in.cluster);
							
							return String.format("(%s;%d;%d)", col.getHeader(), in.columnIndex, in.tableSize);
						}
					}), ","),
					StringUtils.join(Q.project(consequent, new Func<String, FrequentColumnPosition>() {

						@Override
						public String invoke(FrequentColumnPosition in) {
							TableColumn col = Q.firstOrDefault(in.cluster);
							
							return String.format("(%s;%d;%d)", col.getHeader(), in.columnIndex, in.tableSize);
						}
					}), ",")					
					));
		}
	
		return sb.toString();
	}

	Map<Table, List<Collection<TableColumn>>> mapTableToTranslatedTable = null;
	
	public Set<Sequence<Collection<TableColumn>>> calculateSequentialPatternsOfColumnPositions(Collection<Table> tables, Set<Collection<TableColumn>> attributeClusters) {
		
//		Set<Collection<TableColumn>> clusters = getAttributeClusters(new ArrayList<>(tables));
		
		Map<TableColumn, Collection<TableColumn>> mapColumnToCluster = new HashMap<>();
		for(Collection<TableColumn> cluster : attributeClusters) {
			for(TableColumn col : cluster) {
				mapColumnToCluster.put(col, cluster);
			}
		}
		
		// prepare tables: replace columns with their matching clusters
		mapTableToTranslatedTable = new HashMap<>();
		for(Table t : tables) {
			List<Collection<TableColumn>> translated = new ArrayList<>(t.getColumns().size());
			
			for(TableColumn col : t.getColumns()) {
				
				Collection<TableColumn> cluster = mapColumnToCluster.get(col);
				
				if(cluster==null) {
					cluster = new LinkedList<>();
					cluster.add(col);
				}
				
				translated.add(cluster);
				
			}
			
			mapTableToTranslatedTable.put(t, translated);
		}
		
		SequentialPatternMiner<Collection<TableColumn>> seqMiner = new SequentialPatternMiner<Collection<TableColumn>>();
		
		Set<Sequence<Collection<TableColumn>>> patterns = seqMiner.calculateSequentialPatterns(mapTableToTranslatedTable.values());
		
		// remove all patterns that contain unmatched columns (we can never apply them)
		Iterator<Sequence<Collection<TableColumn>>> it = patterns.iterator();
		while(it.hasNext()) {
			Sequence<Collection<TableColumn>> seq = it.next();
			
			if(Q.any(seq.getElements(), new Func<Boolean, Collection<TableColumn>>() {

				@Override
				public Boolean invoke(Collection<TableColumn> in) {
					return in.size()==1;
				}
			})) {
				it.remove();
			}
		}
		
		return patterns;
		
//		// cannot be a map!
//		List<Pair<List<FrequentColumnPosition>, Integer>> patterns = new LinkedList<>();
//		//Map<List<FrequentColumnPosition>, Integer> sequences = new HashMap<>();
//	
//		
//		
//		// create all 1-element sequences
//		for(Collection<TableColumn> cluster : clusters) {
//			// ignore table size as we use relative positions and not column indices
//			FrequentColumnPosition fp = new FrequentColumnPosition(cluster, 0, 0);
//				
//			// create the item set
//			List<FrequentColumnPosition> itemset = new LinkedList<>();
//			itemset.add(fp);
//			
//			patterns.add(new Pair<List<FrequentColumnPosition>, Integer>(itemset, new Integer(cluster.size())));
////			
////			Distribution<Integer> columnIndexDistribution = Distribution.fromCollection(cluster, new Func<Integer, TableColumn>() {
////
////				@Override
////				public Integer invoke(TableColumn in) {
////					return in.getColumnIndex();
////				}
////			});
////			
////			for(final int i : columnIndexDistribution.getElements()) {			
////
////				
////				if(fp.toString().contains("buy")) {
////					System.out.println("test");
////				}
////				
////				sequences.put(itemset, columnIndexDistribution.getFrequency(i));
////
////			}
//		}
//		
//		// easy access to item sets generated in the last round
////		Set<List<FrequentColumnPosition>> OneSequences = new HashSet<>(sequences.keySet());
//		List<Pair<List<FrequentColumnPosition>, Integer>> lastSequences = patterns;
//		List<Pair<List<FrequentColumnPosition>, Integer>> currentSequences = new LinkedList<>();
//
//		// create all 2-element sequences
//		for(Pair<List<FrequentColumnPosition>, Integer> seq1 : patterns) {
//			for(Pair<List<FrequentColumnPosition>, Integer> seq2 : patterns) {
//				if(!seq1.equals(seq2)) {
//					
//					// append seq1 with seq2 (other way around will be done in another iteration, as we have two nested loops over the same list)
//					List<FrequentColumnPosition> mergedSeq = new LinkedList<>(seq1.getFirst());
//					mergedSeq.addAll(seq2.getFirst());
//					
//					Pair<List<FrequentColumnPosition>, Integer> merged = new Pair<List<FrequentColumnPosition>, Integer>(mergedSeq, 0);  
//				}
//			}
//		}
//		
//		// create all i+1-element sequences
//		boolean hasChanges = false;
//		
//		// loop until no new item sets are discovered
//		do {
//			
//			// iterate over all item sets created in the last round
//			for(Pair<List<FrequentColumnPosition>, Integer> sequence1 : lastSequences) {
//				
//				// and combine them with the 1-item sets to create new item sets
//				for(Pair<List<FrequentColumnPosition>, Integer> sequence2 : lastSequences) { 
//					
//					if(!sequence1.equals(sequence2) && !sequence1.containsAll(sequence2)) {
//					
//						// first merge elements in a set to eliminate duplicates
//						Set<FrequentColumnPosition> sequence = new HashSet<>();
//						
//						sequence.addAll(sequence1);
//						sequence.addAll(sequence2);
//						
//						// the sort them to restore the sequence
//						List<FrequentColumnPosition> currentSequence = Q.sort(sequence, new Comparator<FrequentColumnPosition>() {
//
//							@Override
//							public int compare(FrequentColumnPosition o1, FrequentColumnPosition o2) {
//								return Integer.compare(o1.columnIndex, o2.columnIndex);
//							}
//						});
//						
//						boolean valid = true;
//						// then check if it is a valid sequence (currently no gaps allowed)
//						for(int i = 1; i < currentSequence.size(); i++) {
//							if(currentSequence.get(i-1).columnIndex!=currentSequence.get(i).columnIndex-1) {
//								valid=false;
//								break;
//							}
//						}
//						
//						if(valid) {
//							currentSequences.add(currentSequence);
//						}
//					}
//					
//				}
//			}
//			
//
//			// calculate frequency of new item sets
//			// loop over all table schemas
//			for(Table t : tables) {
//				
//				// if the schema contains columns mapped to all clusters of an itemset, increase the count
//				// iterate over all new itemsets
//				for(List<FrequentColumnPosition> sequence : currentSequences) {
//
//					List<TableColumn> schema = new ArrayList<>(t.getSchema().getRecords());
//					
//					// find the longest common subsequence of the table schema and the current sequence
//					
//					boolean isContained = true;
//					for(FrequentColumnPosition fcp : sequence) {
//						if(
//								!(t.getColumns().size()>fcp.columnIndex && fcp.cluster.contains(t.getSchema().get(fcp.columnIndex)))) {
//							isContained=false;
//							break;
//						}
//					}
//					
//					if(isContained) {
//					
//				        int[][] num = new int[schema.size()+1][sequence.size()+1];  //2D array, initialized to 0
//	
//				        //Actual algorithm
//				        for (int i = 1; i <= schema.size(); i++)
//				                for (int j = 1; j <= sequence.size(); j++)
//				                        if (sequence.get(j-1).cluster.contains(schema.get(i-1)))
//				                                num[i][j] = 1 + num[i-1][j-1];
//				                        else
//				                                num[i][j] = Math.max(num[i-1][j], num[i][j-1]);
//						
//			        	if(Q.any(sequence, new Func<Boolean, FrequentColumnPosition>() {
//	
//							@Override
//							public Boolean invoke(FrequentColumnPosition in) {
//								return in.toString().contains("buy");
//							}
//						}) && sequence.size()==1) {
//			        		System.out.println("test");
//			        	}
//				        
//				        // if the longest common subsequence is as long as the current sequence, it is contained in the table schema
//				        if(sequence.size()==num[schema.size()][sequence.size()]) {
//				        	// increment the frequency count of the current sequence
//				        	MapUtils.increment(sequences, sequence);
//				        }
//			        
//					}
//
//				}
//				
//			}
//			
//			// filter the itemsets
//			Iterator<List<FrequentColumnPosition>> it = currentSequences.iterator();
//			while(it.hasNext()) {
//				List<FrequentColumnPosition> itemset = it.next();
//				
//				Integer frequency = sequences.get(itemset);
//				
//				if(frequency==null || frequency<1) {
//					it.remove();
//					sequences.remove(itemset);
//				}
//			}
//			
//			hasChanges = currentSequences.size()>0;
//			lastSequences = currentSequences;
//			currentSequences = new HashSet<>();
//			
//		} while(hasChanges);
//
//		return sequences;
	}
	
	public String formatSequentialPatterns(Set<Sequence<Collection<TableColumn>>> sequences) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Sequential Patterns:\n");
		
		List<Sequence<Collection<TableColumn>>> sorted = Q.sort(sequences, new Comparator<Sequence<Collection<TableColumn>>>() {

			@Override
			public int compare(Sequence<Collection<TableColumn>> o1, Sequence<Collection<TableColumn>> o2) {
				return Integer.compare(o1.getSize(), o2.getSize());
			}
		});
		
		sb.append("c\tf\tseq\n");
		for(Sequence<Collection<TableColumn>> sequence : sorted) {
			sb.append(String.format("%d\t%d\t%s\n", sequence.getSize(), sequence.getCount(), 
					StringUtils.join(Q.project(sequence.getElements(), new Func<String, Collection<TableColumn>>() {

						@Override
						public String invoke(Collection<TableColumn> in) {
							TableColumn col = Q.firstOrDefault(in);
							return String.format("(%s)", col.getHeader());
						}
					}), "->")));
		}
		
		return sb.toString();
	}
	
//	public List<Pair<List<FrequentColumnPosition>, List<FrequentColumnPosition>>> calculateSequentialRulesForColumnPositions(Set<Sequence<Collection<TableColumn>>> sequences) {
//
//		// create sequential rules
//		final List<Pair<List<FrequentColumnPosition>, List<FrequentColumnPosition>>> rules = new LinkedList<>();
//		
//		// iterate over all sequential patterns
//		for(List<FrequentColumnPosition> sequence : sequences.keySet()) {
//			if(sequence.size()>1) {
//
//				// move each item from the condition to the consequent, step by step
//				for(int i = sequence.size()-1; i>0; i--) {
//					List<FrequentColumnPosition> condition = new LinkedList<>(sequence);
//					LinkedList<FrequentColumnPosition> consequent = new LinkedList<>();
//					
//					do {
//						// remove the last element of the  condition
//						FrequentColumnPosition fcp = condition.remove(condition.size()-1);
//						
//						// and add it to the beginning of the consequent
//						consequent.add(0, fcp);
//						
////						System.out.println(String.format("{%s}=>{%s}\n",
////								StringUtils.join(Q.project(condition, new Func<String, FrequentColumnPosition>() {
////
////									@Override
////									public String invoke(FrequentColumnPosition in) {
////										TableColumn col = Q.firstOrDefault(in.cluster);
////										
////										return String.format("(%s;%d;%d)", col.getHeader(), in.columnIndex, in.tableSize);
////									}
////								}), "->"),
////								StringUtils.join(Q.project(consequent, new Func<String, FrequentColumnPosition>() {
////
////									@Override
////									public String invoke(FrequentColumnPosition in) {
////										TableColumn col = Q.firstOrDefault(in.cluster);
////										
////										return String.format("(%s;%d;%d)", col.getHeader(), in.columnIndex, in.tableSize);
////									}
////								}), "->")					
////								));
//					} while(condition.size()>i);
//
//					
////					double confidence = (double)sequences.get(sequence) / (double)sequences.get(condition);
//					
////					if(confidence==1.0) {
//					// we cannot simply add to the map, as the condition can occur multiple times ...
//					rules.add(new Pair<List<FrequentColumnPosition>, List<FrequentColumnPosition>>(condition, consequent));
////						rules.put(condition, consequent);
////					}
//				}
//				
//			}
//		}
//		
//		return rules;
//	}
	
	public String formatSequentialRules(final Set<SequentialRule<Collection<TableColumn>>> rules) {
		StringBuilder sb = new StringBuilder();
		
		// print rules: sort by consequent size
		Collection<SequentialRule<Collection<TableColumn>>> sortedRules = Q.sort(rules, new Comparator<SequentialRule<Collection<TableColumn>>>() {

			@Override
			public int compare(SequentialRule<Collection<TableColumn>> o1, SequentialRule<Collection<TableColumn>> o2) {
				return Integer.compare(o1.getConsequent().getSize(), o2.getConsequent().getSize());
			}
			
		});

		sb.append("Sequential Rules:\n");
		for(SequentialRule<Collection<TableColumn>> rule : sortedRules) {
			
			
			sb.append(String.format("[%.4f] {%s}=>{%s}\n",
					rule.getConfidence(),
					StringUtils.join(Q.project(rule.getCondition().getElements(), new Func<String, Collection<TableColumn>>() {

						@Override
						public String invoke(Collection<TableColumn> in) {
							TableColumn col = Q.firstOrDefault(in);
							
							return String.format("(%s)", col.getHeader());
						}
					}), "->"),
					StringUtils.join(Q.project(rule.getConsequent().getElements(), new Func<String, Collection<TableColumn>>() {

						@Override
						public String invoke(Collection<TableColumn> in) {
							TableColumn col = Q.firstOrDefault(in);
							
							return String.format("(%s)", col.getHeader());
						}
					}), "->")					
					));
			
//			int sequenceSupport = sequences.get(sequence);
//			int consequentSupport = sequences.get(consequent);
			double confidenceInv = (double)rule.getAllElementsSupportCount() / (double)rule.getConsequent().getCount();
			
			if(confidenceInv>1.0) {
				System.out.println("what?");
			}
			
			sb.append(String.format("[%.4f] Inverse {%s}<={%s}\n",
					confidenceInv,
					StringUtils.join(Q.project(rule.getCondition().getElements(), new Func<String, Collection<TableColumn>>() {

						@Override
						public String invoke(Collection<TableColumn> in) {
							TableColumn col = Q.firstOrDefault(in);
							
							return String.format("(%s)", col.getHeader());
						}
					}), "->"),
					StringUtils.join(Q.project(rule.getConsequent().getElements(), new Func<String, Collection<TableColumn>>() {

						@Override
						public String invoke(Collection<TableColumn> in) {
							TableColumn col = Q.firstOrDefault(in);
							
							return String.format("(%s)", col.getHeader());
						}
					}), "->")					
					));
		}
	
		sb.append(formatSequentialRulesGroupedByCondition(rules));
		sb.append(formatSequentialRulesGroupedByConsequent(rules));

		return sb.toString();
	}
	
	public String formatSequentialRulesGroupedByCondition(Set<SequentialRule<Collection<TableColumn>>> rules) {
		StringBuilder sb = new StringBuilder();
		// group rules by condition
		Map<String, Collection<SequentialRule<Collection<TableColumn>>>> grouped = Q.group(rules, new Func<String, SequentialRule<Collection<TableColumn>>>() {

			@Override
			public String invoke(SequentialRule<Collection<TableColumn>> in) {
				return String.format("{%s}", StringUtils.join(Q.project(in.getCondition().getElements(), new Func<String, Collection<TableColumn>>() {

					@Override
					public String invoke(Collection<TableColumn> in) {
						TableColumn col = Q.firstOrDefault(in);
						
						return String.format("(%s)", col.getHeader());
					}
				}), ","));
			}
			
		});
		
		Collection<String> sortedKeys = Q.sort(grouped.keySet());
		
		sb.append("Sequential Rules grouped by condition:\n");
		
		for(String group : sortedKeys) {
			for(SequentialRule<Collection<TableColumn>> rule : grouped.get(group)) {
				sb.append(String.format("[%.4f] %s=>{%s}\n", rule.getConfidence(), group, StringUtils.join(Q.project(rule.getConsequent().getElements(), new Func<String, Collection<TableColumn>>() {

					@Override
					public String invoke(Collection<TableColumn> in) {
						TableColumn col = Q.firstOrDefault(in);
						
						return String.format("(%s)", col.getHeader());
					}
				}), ",")));
			}
		}
		
		return sb.toString();
	}
	
	public String formatSequentialRulesGroupedByConsequent(Set<SequentialRule<Collection<TableColumn>>> rules) {
		StringBuilder sb = new StringBuilder();
		// group rules by consequent
		Map<String, Collection<SequentialRule<Collection<TableColumn>>>> grouped = Q.group(rules, new Func<String, SequentialRule<Collection<TableColumn>>>() {

			@Override
			public String invoke(SequentialRule<Collection<TableColumn>> in) {
				return String.format("{%s}", StringUtils.join(Q.project(in.getConsequent().getElements(), new Func<String, Collection<TableColumn>>() {

					@Override
					public String invoke(Collection<TableColumn> in) {
						TableColumn col = Q.firstOrDefault(in);
						
						return String.format("(%s)", col.getHeader());
					}
				}), ","));
			}
			
		});
		
		Collection<String> sortedKeys = Q.sort(grouped.keySet());
		
		sb.append("Sequential Rules grouped by consequent (inverse rules):\n");
		
		for(String group : grouped.keySet()) {
			for(SequentialRule<Collection<TableColumn>> rule : grouped.get(group)) {
				
				double confidenceInv = (double)rule.getAllElementsSupportCount() / (double)rule.getConsequent().getCount();
				
				sb.append(String.format("[%.4f] %s<={%s}\n", confidenceInv, group, StringUtils.join(Q.project(rule.getCondition().getElements(), new Func<String, Collection<TableColumn>>() {

					@Override
					public String invoke(Collection<TableColumn> in) {
						TableColumn col = Q.firstOrDefault(in);
						
						return String.format("(%s)", col.getHeader());
					}
				}), ",")));
			}
		}
		
		return sb.toString();
	}
}
