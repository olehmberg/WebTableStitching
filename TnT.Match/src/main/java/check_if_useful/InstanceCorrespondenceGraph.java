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
import java.util.Collections;
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
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils2;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRowWithKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class InstanceCorrespondenceGraph {

	private ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences;
	private DataProcessingEngine proc;
	HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys;
	private WebTables web;
	
	/**
	 * 
	 */
	public InstanceCorrespondenceGraph(ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, DataProcessingEngine proc, HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys, WebTables web) {
		this.instanceCorrespondences = instanceCorrespondences;
		this.proc = proc;
		this.tableToCandidateKeys = tableToCandidateKeys;
		this.web = web;
	}
	
	
	/***
	 * 
	 * @param blockedPairs
	 * @param proc
	 * @param tableToSchema
	 * @param tableToCandidateKeys
	 * @return a map table->table->key->mapped rows
	 */
	private Map<Table, Map<Table, Map<String, Set<Integer>>>> printValueBasedLinks(
			ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs, 
			final HashMap<Integer, String> tableToSchema) {
		
		// sort all the groups by (from table),(to table)
    	System.out.println("sorting");
//    	blockedPairs = proc.sort(blockedPairs, new Comparator<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {
//
//			@Override
//			public int compare(
//					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> o1,
//					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> o2) {
//				
//				return o1.getKey().compareTo(o2.getKey());
//			}
//		});
    	
    	blockedPairs = proc.sort(blockedPairs, new Function<String, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(
					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> input) {
				return input.getKey();
			}
    		
    	});
    	
    	Map<String, Map<String, Set<String>>> tableLinksViaKey = new HashMap<>();
    	Map<Table, Map<Table, Map<String, Set<Integer>>>> allLinks = new HashMap<>();
    	
    	// print all the links
    	for(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> group : blockedPairs.get()) {
    		
    		// determine tables from group key
    		String[] ids = group.getKey().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		final Table t1 = web.getTables().get(id1);
    		final Table t2 = web.getTables().get(id2);
    		
    		// count key occurrences
    		HashMap<String, Set<Integer>> keyCounts1 = new HashMap<>();
    		HashMap<String, Set<Integer>> keyCounts2 = new HashMap<>();
    		
    		for(Collection<Integer> key : tableToCandidateKeys.get(id1)) {
    			String s = StringUtils.join(Q.project(key, new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in,  t1.getSchema().get(in).getHeader());
					}
				}), ",");
    			keyCounts1.put(s, new HashSet<Integer>());
    		}
    		for(Collection<Integer> key : tableToCandidateKeys.get(id2)) {
    			String s = StringUtils.join(Q.project(key, new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in, t2.getSchema().get(in).getHeader());
					}
				}), ",");
    			keyCounts2.put(s, new HashSet<Integer>());
    		}
    		
    		System.out.println(String.format("{#%d}%s <-> {#%d}%s", id1, t1.getPath(), id2, t2.getPath()));
    		System.out.println(String.format("%s <-> %s", tableToSchema.get(id1), tableToSchema.get(id2)));
    		
    		ResultSet<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> sorted = proc.sort(group.getRecords(), new Function<Integer, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Integer execute(
						Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> input) {
					return input.getFirst().getRow().getRowNumber();
				}
    			
    		});
    		
    		for(final Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> link : sorted.get()) {
    			
    			String key1 = StringUtils.join(Q.project(link.getFirst().getKey(), new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in,  t1.getSchema().get(in).getHeader());
					}
				}), ",");
    			String key2 = StringUtils.join(Q.project(link.getSecond().getKey(), new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in, t2.getSchema().get(in).getHeader());
					}
				}), ",");
    			
    			if(!keyCounts1.containsKey(key1)) {
    				keyCounts1.put(key1,new HashSet<Integer>());
    			}
    			if(!keyCounts2.containsKey(key2)) {
    				keyCounts2.put(key2,new HashSet<Integer>());
    			}
    			keyCounts1.get(key1).add(link.getFirst().getRow().getRowNumber());
    			keyCounts2.get(key2).add(link.getSecond().getRow().getRowNumber());
    			
    			System.out.println(String.format("{%s}->{%s} : [%s]", key1, key2, StringUtils.join(Q.project(link.getFirst().getKey(), new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return (String)link.getFirst().getRow().get(in);
					}
				}), ",")));
    			System.out.println(String.format("\t[%d] %s\t%s", link.getFirst().getRow().getRowNumber(), link.getFirst().getRow().format(20), link.getFirst().getRow().get(0)));
    			System.out.println(String.format("\t[%d] %s\t%s", link.getSecond().getRow().getRowNumber(), link.getSecond().getRow().format(20), link.getSecond().getRow().get(0)));
    			System.out.println();
    		}
    		
    		Map<String, Set<String>> keys = MapUtils.get(tableLinksViaKey, t1.getPath(), new HashMap<String, Set<String>>());
    		
    		// summarize key counts
    		System.out.println(t1.getPath());
    		int numKeysWithCors = 0;
    		for(String s : keyCounts1.keySet()) {
    			System.out.println(String.format("%s : %d", s, keyCounts1.get(s).size()));
    			
    			if(keyCounts1.get(s).size()>0) {
	    			Set<String> linkedTables = MapUtils.get(keys, s, new HashSet<String>());
	    			linkedTables.add(t2.getPath());
	    			
	    			MapUtils2.put(allLinks, t1, t2, keyCounts1);
	    			numKeysWithCors++;
    			}
    		}
    		System.out.println(t2.getPath());
    		for(String s : keyCounts2.keySet()) {
    			System.out.println(String.format("%s : %d", s, keyCounts2.get(s).size()));
    			
    			MapUtils2.put(allLinks, t2, t1, keyCounts2);
    		}
    		if(numKeysWithCors>1) {
    			System.out.println("* Multiple Matching Candidate Keys *");
    		}
    	}
    	
    	// summarize table linkage via keys
    	System.out.println("Table linkage via keys");
    	for(String t1 : tableLinksViaKey.keySet()) {
    		System.out.println("\t" + t1);
    		Map<String, Set<String>> keys = tableLinksViaKey.get(t1);
    		
    		// count number of keys linking to each table
    		Map<String, Set<String>> tables = new HashMap<>();
    		
    		for(String key : keys.keySet()) {
    			Set<String> linkedTables = keys.get(key);
    			System.out.println(String.format("\t\t%d\t%s\t%s", linkedTables.size(), key, StringUtils.join(linkedTables, ",")));
    			
    			for(String table : linkedTables){
    				Set<String> linkingKeys = MapUtils.get(tables, table, new HashSet<String>());
    				linkingKeys.add(key);
    			}
    		}
    		
    		for(String table : tables.keySet()) {
    			Set<String> linkingKeys = tables.get(table);
    			if(linkingKeys.size()>1) {
    				System.out.println(String.format("\t\t%d\t%s\t%s", linkingKeys.size(), table, StringUtils.join(linkingKeys, "||")));
    			}
    		}
    	}
    	
    	return allLinks;
	}
	
	private static class tableRowPair {
		public int table1;
		public int table2;
		public int row1;
		public int row2;
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + row1;
			result = prime * result + row2;
			result = prime * result + table1;
			result = prime * result + table2;
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			tableRowPair other = (tableRowPair) obj;
			if (row1 != other.row1)
				return false;
			if (row2 != other.row2)
				return false;
			if (table1 != other.table1)
				return false;
			if (table2 != other.table2)
				return false;
			return true;
		}
		
		
	}
	
	private void printValueLinkStatistics(ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs, 
			DataProcessingEngine proc, final int minKeysPerCorrespondence) {

		// group by table ids and keys to get the number of matching keys per correspondence
//		RecordKeyValueMapper<tableRowPair, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> mapToTablesAndRows = new RecordKeyValueMapper<tableRowPair, Group<String,Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>, Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> record,
//					DatasetIterator<Pair<tableRowPair, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> resultCollector) {
//				
//				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : record.getRecords().get()) {
//					
//					List<MatchableTableRow> rows = new ArrayList<>();
//					rows.add(cor.getFirst().getRow());
//					rows.add(cor.getSecond().getRow());
//					Collections.sort(rows, new MatchableTableRow.RowNumberComparator());
//					
//					MatchableTableRow row1 = rows.get(0);
//					MatchableTableRow row2 = rows.get(1);
//					
//					tableRowPair key = new tableRowPair();
//					key.table1 = row1.getTableId();
//					key.row1 = row1.getRowNumber();
//					key.table2 = row2.getTableId();
//					key.row2 = row2.getRowNumber();
//					
//					resultCollector.next(new Pair<tableRowPair, Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>(key, cor));
//					
//				}
//				
//			}
//		};
//		
//		ResultSet<Group<tableRowPair, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> keysByCorrespondences = proc.groupRecords(blockedPairs, mapToTablesAndRows);

		
//		Function<String, Group<tableRowPair, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> tableRowPairToSortingKeyMapper = new Function<String, Group<tableRowPair,Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(Group<tableRowPair, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> input) {
//				return String.format("%d/%d-%d/%d", input.getKey().table1, input.getKey().table2, input.getKey().row1, input.getKey().row2);
//			}
//		}; 
//		keysByCorrespondences = proc.sort(keysByCorrespondences, tableRowPairToSortingKeyMapper);
		
		
		RecordMapper<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>, Pair<String, Pair<Distribution<String>, Distribution<String>>>> recordLinksToDistributionsMapper = new RecordMapper<Group<String,Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>, Pair<String, Pair<Distribution<String>, Distribution<String>>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> record,
					DatasetIterator<Pair<String, Pair<Distribution<String>, Distribution<String>>>> resultCollector) {
				
				Distribution<String> keysPerCorrespondenceDistribution = new Distribution<>();
				Distribution<String> correspondencesPerKeyDistribution = new Distribution<>();
				
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : record.getRecords().get()) {
					
					MatchableTableRowWithKey row1 = cor.getFirst();
					MatchableTableRowWithKey row2 = cor.getSecond();
//					
//					List<MatchableTableRowWithKey> rows = Q.sort(Q.toList(cor.getFirst(), cor.getSecond()), new Comparator<MatchableTableRowWithKey>() {
//
//						@Override
//						public int compare(MatchableTableRowWithKey o1, MatchableTableRowWithKey o2) {
//							return Integer.compare(o1.getRow().getRowNumber(), o2.getRow().getRowNumber());
//						}
//					});
					
					String correspondence = String.format("%d/%d", row1.getRow().getRowNumber(), row2.getRow().getRowNumber());
					String key = String.format("{%s}<->{%s}", row1.getKey(), row2.getKey());

					// we have a new key for 'correspondence'
					keysPerCorrespondenceDistribution.add(correspondence);
					
					// we have a new correspondence for 'key'
					correspondencesPerKeyDistribution.add(key);
				}
				
				if(correspondencesPerKeyDistribution.getElements().size()>minKeysPerCorrespondence) {
					// only interesting if there is more than one matching key
					resultCollector.next(new Pair<String, Pair<Distribution<String>,Distribution<String>>>(record.getKey(), new Pair<Distribution<String>, Distribution<String>>(keysPerCorrespondenceDistribution, correspondencesPerKeyDistribution)));
				}
			}
		};
		ResultSet<Pair<String, Pair<Distribution<String>, Distribution<String>>>> distributions = proc.transform(blockedPairs, recordLinksToDistributionsMapper);
		
		
		
		// sort all the groups by (from table),(to table)
		distributions = proc.sort(distributions, new Function<String, Pair<String, Pair<Distribution<String>, Distribution<String>>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(
					Pair<String, Pair<Distribution<String>, Distribution<String>>> input) {
				return input.getFirst();
			}
    		
    	});
		
		System.out.println("*** Key/Correspondence Distributions for value-based links ***");
    	for(Pair<String, Pair<Distribution<String>, Distribution<String>>> value : distributions.get()) {
    		
    		// determine tables from group key
    		String[] ids = value.getFirst().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		final Table t1 = web.getTables().get(id1);
    		final Table t2 = web.getTables().get(id2);
    		
    		System.out.println(String.format("{#%d}%s/{#%d}%s", id1, t1.getPath(), id2, t2.getPath()));
    		System.out.println("Number of keys per correspondence");
    		System.out.println(value.getSecond().getFirst().format());
    		System.out.println("Number of correspondences per key");
    		System.out.println(value.getSecond().getSecond().format());
    	}
	}
	
	private ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> removeUntrustedValueLinks(
			ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs, 
			DataProcessingEngine proc,
			final HashMap<Integer, String> tableToSchema,
			final HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys) {
		
    	
    	// now filter out untrusted correspondences
		RecordMapper<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> removeUntrustedLinksMapper = new RecordMapper<Group<String,Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> record,
					DatasetIterator<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> resultCollector) {
				
				Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> correspondences = new LinkedList<>(record.getRecords().get()); 
				Integer t1Id = null;
				Integer t2Id = null;
				
				// determine distributions of correspondences per key / keys per correspondence
				Distribution<Collection<Integer>> correspondencesPerKeyDistribution1 = new Distribution<>();
				Distribution<Collection<Integer>> correspondencesPerKeyDistribution2 = new Distribution<>();
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : correspondences) {
					
					if(t1Id == null) {
						t1Id = cor.getFirst().getRow().getTableId();
					}
					if(t2Id == null) {
						t2Id = cor.getSecond().getRow().getTableId();
					}
					
					MatchableTableRowWithKey row1 = cor.getFirst();
					MatchableTableRowWithKey row2 = cor.getSecond();
					
					// we have a new correspondence for 'key'
					correspondencesPerKeyDistribution1.add(row1.getKey());
					correspondencesPerKeyDistribution2.add(row2.getKey());
				}
				
				// find the candidate keys that did not create any correspondences
				Collection<Collection<Integer>> t1MissingKeys = new LinkedList<>();
				for(Collection<Integer> candKey : tableToCandidateKeys.get(t1Id)) {
					if(correspondencesPerKeyDistribution1.getFrequency(candKey)==0) {
						t1MissingKeys.add(candKey);
					}
				}
				Collection<Collection<Integer>> t2MissingKeys = new LinkedList<>();
				for(Collection<Integer> candKey : tableToCandidateKeys.get(t2Id)) {
					if(correspondencesPerKeyDistribution2.getFrequency(candKey)==0) {
						t2MissingKeys.add(candKey);
					}
				}
				
				// try to find the values for that candidate key in the corresponding records (this time not only in the candidate keys, but in all attributes)
				Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> newCorrespondences = new LinkedList<>();
				// keep track of the checked rows, as we have duplicate correspondences
				final Set<String> checkedCors = new HashSet<>();
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : correspondences) {
					String corString = String.format("%s-%s", cor.getFirst().getRow().getRowNumber(), cor.getSecond().getRow().getRowNumber());
					if(!checkedCors.contains(corString)) {
						
						// check keys from table1
						for(Collection<Integer> candKey : t1MissingKeys) {
							
							// get the key values for candKey in table1
							LinkedList<String> t1Values = new LinkedList<>();
							for(Integer index : candKey) {
								Object value = cor.getFirst().getRow().get(index);
								String stringValue = null;
								if(value==null) {
									stringValue = "NULL";
								} else {
									stringValue = value.toString();
								}
								t1Values.add(stringValue);
							}
							
							// try to find the values in table2
							Collection<Integer> matchingValueIndices = new HashSet<>();
							MatchableTableRow row2 = cor.getSecond().getRow();
							for(int i = 0; i < row2.getRowLength(); i++) {
								Object value = row2.get(i);
								String stringValue = null;
								if(value==null) {
									stringValue = "NULL";
								} else {
									stringValue = value.toString();
								}
								
								if(t1Values.contains(stringValue)) {
									matchingValueIndices.add(i);
									t1Values.remove(stringValue);
								}
							}
							
							// have all values been found?
							if(t1Values.size()==0) {
								// if yes, create a new correspondence
								Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> newCor = new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(
										new MatchableTableRowWithKey(cor.getFirst().getRow(), candKey), 
										new MatchableTableRowWithKey(cor.getSecond().getRow(), matchingValueIndices));
								newCorrespondences.add(newCor);
							}
						}
						
						// check keys from table2
						for(Collection<Integer> candKey : t2MissingKeys) {
							
							// get the key values for candKey in table2
							LinkedList<String> t2Values = new LinkedList<>();
							for(Integer index : candKey) {
								Object value = cor.getSecond().getRow().get(index);
								String stringValue = null;
								if(value==null) {
									stringValue = "NULL";
								} else {
									stringValue = value.toString();
								}
								t2Values.add(stringValue);
							}
							
							// try to find the values in table2
							Collection<Integer> matchingValueIndices = new HashSet<>();
							MatchableTableRow row1 = cor.getFirst().getRow();
							for(int i = 0; i < row1.getRowLength(); i++) {
								Object value = row1.get(i);
								String stringValue = null;
								if(value==null) {
									stringValue = "NULL";
								} else {
									stringValue = value.toString();
								}
								
								if(t2Values.contains(stringValue)) {
									matchingValueIndices.add(i);
									t2Values.remove(stringValue);
								}
							}
							
							// have all values been found?
							if(t2Values.size()==0) {
								// if yes, create a new correspondence
								Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> newCor = new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(
										new MatchableTableRowWithKey(cor.getFirst().getRow(), matchingValueIndices), 
										new MatchableTableRowWithKey(cor.getSecond().getRow(), candKey));
								newCorrespondences.add(newCor);
							}
						}
						
						checkedCors.add(corString);
					}
				}
				
				// add the new correspondences to the existing ones
				correspondences.addAll(newCorrespondences);
				
				// determine the frequency of candidate key combinations 
				SimilarityMatrix<WebTableKey> keyCombinations = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
				final HashMap<Collection<Integer>, Collection<Integer>> trustedKeyCombinations = new HashMap<>();
				
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : correspondences) {
					
					MatchableTableRowWithKey row1 = cor.getFirst();
					MatchableTableRowWithKey row2 = cor.getSecond();
					
					// we have a new key combination
					WebTableKey k1 = new WebTableKey(row1.getRow().getTableId(), new HashSet<>(row1.getKey()));
					WebTableKey k2 = new WebTableKey(row2.getRow().getTableId(), new HashSet<>(row2.getKey()));
					Double last = keyCombinations.get(k1,k2);
					if(last==null) {
						last = 0.0;
					}
					keyCombinations.set(k1, k2, last+1.0);
				}
				
				// determine the trusted combinations
				// if key1 is combined with more than one other key, only one can be correct
				// we simply trust the most frequent combination
				BestChoiceMatching bcm = new BestChoiceMatching();
				keyCombinations = bcm.match(keyCombinations);
				
				for(WebTableKey k1 : keyCombinations.getFirstDimension()) {
					for(WebTableKey k2 : keyCombinations.getMatches(k1)) {
						trustedKeyCombinations.put(k1.getColumnIndices(), k2.getColumnIndices());
					}
				}
				
				// only keep correspondences from trusted keys
				correspondences = Q.where(correspondences, new Func<Boolean, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>() {

					@Override
					public Boolean invoke(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor) {
						MatchableTableRowWithKey row1 = cor.getFirst();
						MatchableTableRowWithKey row2 = cor.getSecond();
						
						Set<Integer> k1 = new HashSet<>(row1.getKey());
						Set<Integer> k2 = new HashSet<>(row2.getKey());
						return trustedKeyCombinations.containsKey(k1)
								&& trustedKeyCombinations.get(k1).equals(k2);
					}
				});
				
				// determine distributions of correspondences per key / keys per correspondence
				final Distribution<String> keysPerCorrespondenceDistribution = new Distribution<>();
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : correspondences) {
					
					MatchableTableRowWithKey row1 = cor.getFirst();
					MatchableTableRowWithKey row2 = cor.getSecond();
					
					String correspondence = String.format("%d/%d", row1.getRow().getRowNumber(), row2.getRow().getRowNumber());

					// we have a new key for 'correspondence'
					keysPerCorrespondenceDistribution.add(correspondence);
				}
				

				
				
//				if(correspondencesPerKeyDistribution.getElements().size()>1) {
					// only interesting if there is more than one matching key
					
				correspondences = Q.where(correspondences, new Func<Boolean, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>(){

					@Override
					public Boolean invoke(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor) {
						MatchableTableRowWithKey row1 = cor.getFirst();
						MatchableTableRowWithKey row2 = cor.getSecond();
						
						String correspondence = String.format("%d/%d", row1.getRow().getRowNumber(), row2.getRow().getRowNumber());
						
						// only keep a record if it was created by all trusted keys
						return keysPerCorrespondenceDistribution.getFrequency(correspondence)==trustedKeyCombinations.size();
					}});
					
					if(correspondences.size()>0) {
						// if there are any links left, keep them
//						resultCollector.next(record);
						
						// but deduplicate first
						HashMap<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> existing = new HashMap<>();
						Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> oldRecords = correspondences;
						
						ResultSet<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> newRecords = new ResultSet<>();
						
						for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor : oldRecords) {

							List<MatchableTableRowWithKey> rows = Q.toList(cor.getFirst(), cor.getSecond());
							Collections.sort(rows, new MatchableTableRowWithKey.RowNumberComparator());
							
							MatchableTableRowWithKey row1 = rows.get(0);
							MatchableTableRowWithKey row2 = rows.get(1);

							String correspondence = String.format("%d/%d", row1.getRow().getRowNumber(), row2.getRow().getRowNumber());
							
							if(existing.containsKey(correspondence)) {
								// add the key indices to the existing correspondence
								Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> existingCor = existing.get(correspondence);
								existingCor.getFirst().getKey().addAll(cor.getFirst().getKey());
								existingCor.getSecond().getKey().addAll(cor.getSecond().getKey());
							} else {
								existing.put(correspondence, 
										new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(
												new MatchableTableRowWithKey(cor.getFirst().getRow(), new HashSet<>(cor.getFirst().getKey())), 
												new MatchableTableRowWithKey(cor.getSecond().getRow(), new HashSet<>(cor.getSecond().getKey()))));
								newRecords.add(existing.get(correspondence));
							}
						}
//						
						resultCollector.next(new Group<String, Pair<MatchableTableRowWithKey,MatchableTableRowWithKey>>(record.getKey(), newRecords));
					}
					
//				}
			}
		};
		
		return proc.transform(blockedPairs, removeUntrustedLinksMapper);
	}

	private ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> verifyValueLinkTransitivity(
			ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs, 
			DataProcessingEngine proc,
			final HashMap<Integer, String> tableToSchema,
			final HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys) {
		
		Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> toRemove = new LinkedList<>();
		Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> toAdd = new LinkedList<>();
		
		Map<Integer, Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>> linksByTables = new HashMap<>();
		
		for(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> group : blockedPairs.get()) {
    		// determine the tables from the grouping key
    		String[] ids = group.getKey().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		Table t1 = web.getTables().get(id1);
    		Table t2 = web.getTables().get(id2);   	
    		
    		MapUtils2.put(linksByTables, id1, id2, group.getRecords().get());
		}
		
		
		// table -> table -> row -> { correspondences }
		Map<Integer, Map<Integer, Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>>> linksByRecord = new HashMap<>();
		// table -> row -> { correspondences }
		Map<Integer, Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>> linksFromTable = new HashMap<>();
		// for each table
		for(Integer t1 : linksByTables.keySet()) {
		//  check all record links to other tables by record
			Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> recordLinksFromTable = MapUtils.get(linksFromTable, t1, new HashMap<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>());
			
			for(Integer t2 : linksByTables.get(t1).keySet()) {
				
				Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> recordLinks = MapUtils2.get(linksByRecord, t1, t2);
				if(recordLinks==null) {
					recordLinks=new HashMap<>();
					MapUtils2.put(linksByRecord, t1, t2, recordLinks);
				}
				
				for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> link : linksByTables.get(t1).get(t2)) {
				
					// links between t1 and t2
					Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> links = recordLinks.get(link.getFirst().getRow().getRowNumber());
					if(links==null) {
						links = new LinkedList<>();
						recordLinks.put(link.getFirst().getRow().getRowNumber(), links);
					}
					links.add(link);
					
					// links from t1
					links = recordLinksFromTable.get(link.getFirst().getRow().getRowNumber());
					if(links==null) {
						links = new LinkedList<>();
						recordLinksFromTable.put(link.getFirst().getRow().getRowNumber(), links);
					}
					links.add(link);
				}
			}
		}
		
		for(Integer t1 : linksFromTable.keySet()) {
		//   if a record has correspondences in two other tables, check if the transitive connection exists
			
			Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> tableLinks = linksFromTable.get(t1);
			
			for(Integer row : tableLinks.keySet()) {
				Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> links = tableLinks.get(row);
				
				if(links.size()>1) {
					
					List<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> linkList = new ArrayList<>(links);
					
					for(int i = 0; i < linkList.size(); i++) {
						Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> corA = linkList.get(i);
						int tA = corA.getSecond().getRow().getTableId();
						int rA = corA.getSecond().getRow().getRowNumber();
						
						Map<Integer, Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>> linksA = linksByRecord.get(tA);
						
						for(int j = i+1; j < linkList.size(); j++) {
							Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> corB = linkList.get(j);
							int tB = corB.getSecond().getRow().getTableId();
							int rB = corB.getSecond().getRow().getRowNumber();
							
							if(tA!=tB) {
								boolean found = false;
								
								Map<Integer, Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> linksAB = linksA.get(tB);
								
								if(linksAB!=null) {
									Collection<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> recordLinks = linksAB.get(rA);
									
									if(recordLinks!=null) {
										for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> link : recordLinks) {
											if(link.getSecond().getRow().getRowNumber()==rB) {
												found = true;
												break;
											}
										}
									}
								}
								
								if(!found) {
									
									// check if there should be a correspondence
									Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> newCor = null;
									boolean created = false;
									for(Collection<Integer> candidateKey : tableToCandidateKeys.get(tA)) {
										newCor = matches(candidateKey, corA.getSecond().getRow(), corB.getSecond().getRow());
										if(newCor!=null) {
											toAdd.add(newCor);
											toAdd.add(new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(
													new MatchableTableRowWithKey(newCor.getSecond().getRow(), newCor.getSecond().getKey()), 
													new MatchableTableRowWithKey(newCor.getFirst().getRow(), newCor.getSecond().getKey())));
											System.out.println(String.format("&&& Adding {#%d}[#%d]{%s}<->{#%d}[#%d]{%s}<->{#%d}[#%d]{%s} (via transitivity)", tA, rA, newCor.getFirst().getKey(), t1, row, corA.getFirst().getKey(), tB, rB, newCor.getSecond().getKey()));
											System.out.println(String.format("\t{%d}[%d]\t%s", tA, rA, newCor.getFirst().getRow().format(20)));
											System.out.println(String.format("\t{%d}[%d]\t%s", t1, row, corA.getFirst().getRow().format(20)));
											System.out.println(String.format("\t{%d}[%d]\t%s", tB, rB, newCor.getSecond().getRow().format(20)));
											created = true;
										}
									}
									
									
									if(!created) {
										//     if not, remove both record links, as they are incorrect (the transitive connection must have been checked before) 
										toRemove.add(corA);
										toRemove.add(corB);
										
										System.out.println(String.format("&&& Removing {#%d}[#%d]{%s}<->{#%d}[#%d]{%s}<->{#%d}[#%d]{%s} (violated transitivity)", tA, rA, corA.getSecond().getKey(), t1, row, corA.getFirst().getKey(), tB, rB, corB.getSecond().getKey()));
										System.out.println(String.format("\t{%d}[%d]\t%s", tA, rA, corA.getSecond().getRow().format(20)));
										System.out.println(String.format("\t{%d}[%d]\t%s", t1, row, corA.getFirst().getRow().format(20)));
										System.out.println(String.format("\t{%d}[%d]\t%s", tB, rB, corB.getSecond().getRow().format(20)));
									}
								}
							}
						}
					}
					
				}
			}
		}
			
		Iterator<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> it = blockedPairs.get().iterator();
		
		while(it.hasNext()) {
			Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> group = it.next();
			
    		String[] ids = group.getKey().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		
    		Iterator<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> toAddIt = toAdd.iterator();
    		while(toAddIt.hasNext()) {
    			Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> cor = toAddIt.next();
    			if(cor.getFirst().getRow().getTableId()==id1 && cor.getSecond().getRow().getTableId()==id2) {
    				group.getRecords().add(cor);
    				toAddIt.remove();
    			}
    		}
			
			group.getRecords().get().removeAll(toRemove);
			if(group.getRecords().size()==0) {
				it.remove();
			}
		}
		
		return blockedPairs;
	}
	
	public Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> matches(Collection<Integer> candKey, MatchableTableRow record1, MatchableTableRow record2) {
		
		// get the key values for candKey in table1
		LinkedList<String> t1Values = new LinkedList<>();
		for(Integer index : candKey) {
			Object value = record1.get(index);
			String stringValue = null;
			if(value==null) {
				stringValue = "NULL";
			} else {
				stringValue = value.toString();
			}
			t1Values.add(stringValue);
		}
		
		// try to find the values in table2
		Collection<Integer> matchingValueIndices = new HashSet<>();
		MatchableTableRow row2 = record2;
		for(int i = 0; i < row2.getRowLength(); i++) {
			Object value = row2.get(i);
			String stringValue = null;
			if(value==null) {
				stringValue = "NULL";
			} else {
				stringValue = value.toString();
			}
			
			if(t1Values.contains(stringValue)) {
				matchingValueIndices.add(i);
				t1Values.remove(stringValue);
			}
		}
		
		// have all values been found?
		if(t1Values.size()==0) {
			// if yes, create a new correspondence
			Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> newCor = new Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>(
					new MatchableTableRowWithKey(record1, candKey), 
					new MatchableTableRowWithKey(record2, matchingValueIndices));
			return newCor;
		} else {
			return null;
		}
	}
	
}
