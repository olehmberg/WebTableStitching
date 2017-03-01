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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils2;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Pair;
import de.uni_mannheim.informatik.dws.t2k.utils.data.graph.Graph;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.P;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRowWithKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableMatchingKey;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.SchemaFuser;
import de.uni_mannheim.informatik.dws.tnt.mining.SequentialPatternMiner;
import de.uni_mannheim.informatik.dws.tnt.mining.SequentialPatternMiner.Sequence;
import de.uni_mannheim.informatik.dws.tnt.mining.SequentialPatternMiner.SequentialRule;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTablesMatchingGraph {

	// all tables in this graph
	private Set<Table> allTables;
	/**
	 * @return the allTables
	 */
	public Set<Table> getAllTables() {
		return allTables;
	}
	
	public WebTablesMatchingGraph(Collection<Table> tables) {
		allTables = new HashSet<>(tables);
	}
	
	// instance correspondences
	private Map<Table, Map<Table, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>>>> instanceCorrespondences = new HashMap<>();

	// schema correspondences
	private Map<Table, Map<Table, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>>> schemaCorrespondences = new HashMap<>();

	// valid and invalid join keys between tables
	private Map<Table, Map<Table, Set<WebTableMatchingKey>>> matchingKeys = new HashMap<>();
	
	// un-matchable headers
	private Map<String, Set<String>> disjointHeaders = new HashMap<>();
	/**
	 * @param disjointHeaders the disjointHeaders to set
	 */
	public void setDisjointHeaders(Map<String, Set<String>> disjointHeaders) {
		this.disjointHeaders = disjointHeaders;
	}
	
	//TODO store discovered header synonyms

	public void addInstanceCorrespondence(Table t1, Table t2, MatchableTableRowWithKey row1, MatchableTableRowWithKey row2, double similarity) {
		// add correspondence in both directions
		ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> cors1 = MapUtils2.get(instanceCorrespondences, t1, t2, new ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>>());
		ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> cors2 = MapUtils2.get(instanceCorrespondences, t2, t1, new ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>>());
		
		cors1.add(new Correspondence<MatchableTableRow, MatchableTableColumn>(row1.getRow(), row2.getRow(), 1.0, null));
		cors2.add(new Correspondence<MatchableTableRow, MatchableTableColumn>(row2.getRow(), row1.getRow(), 1.0, null));
	}
	
	public void addInstanceCorrespondence(Table t1, Table t2, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceMapping) {
		if(instanceMapping!=null) {
			// add correspondence in both directions
			MapUtils2.put(instanceCorrespondences, t1, t2, instanceMapping);
			MapUtils2.put(instanceCorrespondences, t2, t1, Correspondence.changeDirection(instanceMapping));
		}
	}
	
	public List<Table> getTablesWithInstanceCorrespondences() {
		return new ArrayList<>(instanceCorrespondences.keySet());
	}
	
	public List<Table> getTablesConnectedViaInstanceCorrespondences(Table t) {
		if(instanceCorrespondences.containsKey(t)) {
			return new ArrayList<>(instanceCorrespondences.get(t).keySet());
		} else {
			return null;
		}
	}
	
	public ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> getInstanceCorrespondences(Table from, Table to) {
		if(instanceCorrespondences.containsKey(from)) {
			return instanceCorrespondences.get(from).get(to);
		} else {
			return null;
		}
	}
	
	public void removeInstanceCorrespondences(Table t1, Table t2) {
		// remove correspondences in both directions
		MapUtils2.remove(instanceCorrespondences, t1, t2);
		MapUtils2.remove(instanceCorrespondences, t2, t1);
	}
	
	/**
	 * 
	 * @param t1
	 * @param t2
	 * @param correspondence
	 * @return true, if the correspondence was added
	 */
	public boolean addSchemaCorrespondence(Table t1, Table t2, Correspondence<MatchableTableColumn, MatchableTableRow> correspondence) {
		if(!isInvalidSchemaCorrespondence(t1, t2, correspondence)) {
			
			// does it exist already?
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> existing = getSchemaCorrespondences(t1, t2);
			if(existing!=null) {
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : existing.get()) {
					
//					MatchableTableColumn c1 = correspondence.getFirstRecord();
//					MatchableTableColumn c2 = correspondence.getSecondRecord();
					
//					if(cor.getFirstRecord().getTableId()==c1.getTableId() && cor.getFirstRecord().getColumnIndex()==c1.getColumnIndex()
//						&& cor.getSecondRecord().getTableId()==c2.getTableId() && cor.getSecondRecord().getColumnIndex()==c2.getColumnIndex()) {
					if(cor.getFirstRecord().equals(correspondence.getFirstRecord()) && cor.getSecondRecord().equals(correspondence.getSecondRecord())
					|| cor.getSecondRecord().equals(correspondence.getFirstRecord()) && cor.getFirstRecord().equals(correspondence.getSecondRecord())) {
						return false;
					}
				}
			}
			
			// add correspondence in both directions
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> cors1 = MapUtils2.get(schemaCorrespondences, t1, t2, new ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>());
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> cors2 = MapUtils2.get(schemaCorrespondences, t2, t1, new ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>());
			
			cors1.add(new Correspondence<MatchableTableColumn, MatchableTableRow>(correspondence.getFirstRecord(), correspondence.getSecondRecord(), correspondence.getSimilarityScore(), correspondence.getCausalCorrespondences()));
			cors2.add(new Correspondence<MatchableTableColumn, MatchableTableRow>(correspondence.getSecondRecord(), correspondence.getFirstRecord(), correspondence.getSimilarityScore(), correspondence.getCausalCorrespondences()));
		
			cors1.deduplicate();
			cors2.deduplicate();
			
			return true;
		} else {
			return false;
		}
	}
	
	public void addSchemaCorrespondences(Table t1, Table t2, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping) {
		removeInvalidSchemaCorrespondences(t1, t2, schemaMapping);
		
		// add correspondence in both directions
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> cors1 = MapUtils2.get(schemaCorrespondences, t1, t2, new ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>());
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> cors2 = MapUtils2.get(schemaCorrespondences, t2, t1, new ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>());
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaMapping.get()) {
			cors1.add(new Correspondence<MatchableTableColumn, MatchableTableRow>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), cor.getCausalCorrespondences()));
			cors2.add(new Correspondence<MatchableTableColumn, MatchableTableRow>(cor.getSecondRecord(), cor.getFirstRecord(), cor.getSimilarityScore(), cor.getCausalCorrespondences()));
		}
		
		cors1.deduplicate();
		cors2.deduplicate();
//		MapUtils2.put(schemaCorrespondences, t1, t2, schemaMapping);
	}
	
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> getSchemaCorrespondences(Table t1, Table t2) {
		if(schemaCorrespondences.containsKey(t1) && schemaCorrespondences.get(t1).containsKey(t2)) {
			return schemaCorrespondences.get(t1).get(t2);
		} else { 
			return null;
		}
	}
	
	public boolean isInvalidSchemaCorrespondence(Table t1, Table t2, Correspondence<MatchableTableColumn, MatchableTableRow> correspondence) {
		TableColumn colT1 = t1.getSchema().get(correspondence.getFirstRecord().getColumnIndex());
		TableColumn colT2 = t2.getSchema().get(correspondence.getSecondRecord().getColumnIndex());
		
		// remove the correspondence if it is between two columns with disjoint headers (which cannot have a correspondence)
		if(disjointHeaders.containsKey(colT1.getHeader()) && disjointHeaders.get(colT1.getHeader()).contains(colT2.getHeader())
				|| disjointHeaders.containsKey(colT2.getHeader()) && disjointHeaders.get(colT2.getHeader()).contains(colT1.getHeader())) {
			return true;
		} else {
			return false;
		}
	}
	
	public void removeInvalidSchemaCorrespondences(Table t1, Table t2, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping) {
		LinkedList<Correspondence<MatchableTableColumn, MatchableTableRow>> invalidSchemaCorrespondences = new LinkedList<>();
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaMapping.get()) {
			
			// remove the correspondence if it is between two columns with disjoint headers (which cannot have a correspondence)
			if(isInvalidSchemaCorrespondence(t1, t2, cor)) {
				invalidSchemaCorrespondences.add(cor);
//				mergeLog.append(String.format("Schema Correspondence %s->%s ignored as the column headers appeared in the same table\n", colT2.getHeader(), colT1.getHeader()));
			} else {
				// add the correspondences to the graph for schema clustering
//				schemaClusters.addEdge(new Triple<TableColumn, TableColumn, Double>(colT2, colT1, cor.getSimilarityScore()));
			}
		}
		schemaMapping.remove(invalidSchemaCorrespondences);
	}
	
	public List<Table> getTablesWithSchemaCorrespondences() {
		return new ArrayList<>(schemaCorrespondences.keySet());
	}
	
	public List<Table> getTablesConnectedViaSchemaCorrespondences(Table t) {
		if(schemaCorrespondences.containsKey(t)) {
			return new ArrayList<>(schemaCorrespondences.get(t).keySet());
		} else {
			return null;
		}
	}

	public Map<TableColumn, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>> getSchemaCorrespondecesPerAttribute(Table t) {
		Map<Table, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>> correspondences = schemaCorrespondences.get(t);
		
		Map<TableColumn, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>> result = new HashMap<>();
		
		if(correspondences!=null) {
			for(Table tbl : correspondences.keySet()) {
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : correspondences.get(tbl).get()) {
					
					TableColumn col = t.getSchema().get(cor.getFirstRecord().getColumnIndex());
					
					ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> cors = MapUtils.get(result, col, new ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>());
					
					cors.add(cor);
				}
			}
		}
		
		return result;
	}
	
	public void addMatchingKeys(Table t1, Table t2, Set<WebTableMatchingKey> matchingKey) {
		MapUtils2.put(matchingKeys, t1, t2, matchingKey);
		
		Set<WebTableMatchingKey> inverted = new HashSet<>(matchingKey.size());
		for(WebTableMatchingKey p : matchingKey) {
			inverted.add(p.invert());
		}
		
		MapUtils2.put(matchingKeys, t2, t1, inverted);
	}
	
	public Set<WebTableMatchingKey> getMatchingKeys(Table t1, Table t2) {
		return MapUtils2.get(matchingKeys, t1, t2);
	}
	
	public void removeMatchingKeys(Table t1, Table t2) {
		MapUtils2.remove(matchingKeys, t1, t2);
		MapUtils2.remove(matchingKeys, t2, t1);
	}
	
	public List<Table> getTablesWithKeyCorrespondences() {
		return new ArrayList<>(matchingKeys.keySet());
	}
	
	public List<Table> getTablesConnectedViaKeyCorrespondences(Table t) {
		if(matchingKeys.containsKey(t)) {
			return new ArrayList<>(matchingKeys.get(t).keySet());
		} else {
			return null;
		}
	}
	
	public String formatMatchingGraph() {
		StringBuilder sb = new StringBuilder();
		ConnectedComponentClusterer<TableColumn> schemaClusters = new ConnectedComponentClusterer<>();
    	ConnectedComponentClusterer<Table> tableComponents = new ConnectedComponentClusterer<>();
    	Map<String, Collection<String>> attributeNameIndex = getAttributeNameClusterIndex(schemaCorrespondences.keySet());
    	Map<Table, Integer> tableToCluster = new HashMap<>();
		
		sb.append("### Web Tables Matching Graph ###\n");
		
		Set<Collection<Table>> clusters = getConnectedTables();
		
		Distribution<Integer> clusterSizes = Distribution.fromCollection(clusters, new Func<Integer, Collection<Table>>() {

			@Override
			public Integer invoke(Collection<Table> in) {
				return in.size();
			}
		});
		
		sb.append("Cluster size distribution\n");
		sb.append(clusterSizes.format() + "\n");
		
		int idx = 0;
		for(Collection<Table> clu : clusters) {
			ConnectedComponentClusterer<TableColumn> localSchemaClusters = new ConnectedComponentClusterer<>();
			
			sb.append(String.format("*** Cluster %d: %d tables\n", idx, clu.size()));
			
			List<Table> list = new ArrayList<>(clu);
			
			for(int i = 0; i < list.size(); i++) {
				Table t1 = list.get(i);
				
				tableToCluster.put(t1, idx);
				
				for(TableColumn c : t1.getColumns()) {
					if(!SpecialColumns.isSpecialColumn(c) && !c.getHeader().equals("null")) {
						localSchemaClusters.addEdge(new Triple<TableColumn, TableColumn, Double>(c, c, 1.0));
						schemaClusters.addEdge(new Triple<TableColumn, TableColumn, Double>(c, c, 1.0));
						
					}
				}
				
				sb.append(String.format("[%d] %s\n", t1.getTableId(), t1.getPath()));
				
				//print candidate keys
				sb.append(" Candidate Keys: \n");
				for(Collection<TableColumn> candKey : t1.getSchema().getCandidateKeys()) {
					sb.append(String.format(" {%s}\n", StringUtils.join(Q.project(candKey, new TableColumn.ColumnHeaderProjection()), ",")));
				}
				
				for(int j = i+1; j < list.size(); j++) {
					Table t2 = list.get(j);
					ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = getInstanceCorrespondences(t1, t2);
					int numInst = instanceCorrespondences==null ? 0 : instanceCorrespondences.size();
					
//					Table tt1 = t1;
//					Table tt2 = t2;
					ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = getSchemaCorrespondences(t1, t2);
//					if(schemaCorrespondences==null) {
//						tt1 = t2;
//						tt2 = t1;
//						schemaCorrespondences = getSchemaCorrespondences(tt1, tt2);
//					}
					
					Collection<WebTableMatchingKey> matchingKeys = getMatchingKeys(t1, t2);
					if(matchingKeys==null) {
//						System.out.println("Missing matching key!");
//						matchingKeys = getMatchingKeys(t2, t1);
					}
					
					if(instanceCorrespondences!=null || schemaCorrespondences!=null) {

						sb.append(String.format(" <-> [%d] %s %d instance correspondences\n", t2.getTableId(), t2.getPath(), numInst));
						
						sb.append("  Candidate Keys: \n");
						for(Collection<TableColumn> candKey : t2.getSchema().getCandidateKeys()) {
							sb.append(String.format("   {%s}\n", StringUtils.join(Q.project(candKey, new TableColumn.ColumnHeaderProjection()), ",")));
						}
						
						HashSet<TableColumn> t1cols = new HashSet<>(t1.getColumns());
						HashSet<TableColumn> t2cols = new HashSet<>(t2.getColumns());
						
						// remove special columns
						SpecialColumns.removeSpecialColumns(t1cols);
						SpecialColumns.removeSpecialColumns(t2cols);
						
						sb.append("  Schema Correspondences:\n");
						
						if(schemaCorrespondences!=null) {
							for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
								TableColumn c1;
								TableColumn c2;
								c1 = t1.getSchema().get(cor.getFirstRecord().getColumnIndex());
								c2 = t2.getSchema().get(cor.getSecondRecord().getColumnIndex());
								t1cols.remove(c1);
								t2cols.remove(c2);
//								if(t1==tt1) {
//									c1 = tt1.getSchema().get(cor.getFirstRecord().getColumnIndex());
//									c2 = tt2.getSchema().get(cor.getSecondRecord().getColumnIndex());
//									t1cols
//								} else {
//									c2 = tt1.getSchema().get(cor.getFirstRecord().getColumnIndex());
//									c1 = tt2.getSchema().get(cor.getSecondRecord().getColumnIndex());
//								}
								sb.append(String.format("   (%.8f) [%d] %s <-> [%d] %s\n", cor.getSimilarityScore(), c1.getColumnIndex(), c1.getHeader(), c2.getColumnIndex(), c2.getHeader()));
								
								// add the correspondences to the graph for schema clustering
								schemaClusters.addEdge(new Triple<TableColumn, TableColumn, Double>(c1, c2, cor.getSimilarityScore()));
								localSchemaClusters.addEdge(new Triple<TableColumn, TableColumn, Double>(c1, c2, cor.getSimilarityScore()));
								tableComponents.addEdge(new de.uni_mannheim.informatik.wdi.model.Triple<Table, Table, Double>(t1, t2, 1.0));
							}
						} else {
							sb.append("   no schema correspondences\n");
						}
						// print the unmatched columns to figure out why correspondences are missing!
						if(t1cols.size()>0) {
							sb.append(String.format("   unmatched columns in %d: %s\n", t1.getTableId(), StringUtils.join(Q.project(t1cols, new TableColumn.ColumnHeaderProjection()), ",")));
						}
						if(t2cols.size()>0) {
							sb.append(String.format("   unmatched columns in %d: %s\n", t2.getTableId(), StringUtils.join(Q.project(t2cols, new TableColumn.ColumnHeaderProjection()), ",")));
						}
						for(TableColumn c1 : t1cols) {
							Collection<String> clu1 = attributeNameIndex.get(c1.getHeader());
							if(clu1!=null) {
								for(TableColumn c2 : t2cols) {
//									if(c1.getTable().getTableId()==5&&c2.getTable().getTableId()==25) {
//										System.out.println("test");
//									}
									Collection<String> clu2 = attributeNameIndex.get(c2.getHeader());
									if(clu2!=null) {
										if(clu1.contains(c2.getHeader()) || clu2.contains(c1.getHeader())) {
											sb.append(String.format("   missing correspondence: {%d}[%d]%s<->{%d}[%d]%s\t\t{%s}<->{%s}\n", t1.getTableId(), c1.getColumnIndex(), c1.getHeader(), t2.getTableId(), c2.getColumnIndex(), c2.getHeader(), c1.getIdentifier(), c2.getIdentifier()));
										}
									}
								}
							}
						}
						
						sb.append("  Matching Keys:\n");
						if(matchingKeys!=null) {
							for(WebTableMatchingKey matchingKey : matchingKeys) {
								if(matchingKey.getFirst().size()!=matchingKey.getSecond().size()) {
									System.out.println("Invalid matching key size!");
								}
								
								sb.append(String.format("   {%s} <-> {%s}\n", 
										StringUtils.join(Q.project(matchingKey.getFirst(), new TableColumn.ColumnHeaderProjection()), ","),
										StringUtils.join(Q.project(matchingKey.getSecond(), new TableColumn.ColumnHeaderProjection()), ",")
										));
							}
						}
						
						if(instanceCorrespondences!=null) {
							for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : instanceCorrespondences.get()) {
								sb.append(String.format("   %s\n", cor.getFirstRecord().format(20)));
								sb.append(String.format("   %s\n", cor.getSecondRecord().format(20)));
							}
						}
					}
				}
			}
			
			sb.append("Row Clusters:\n");
			int cluNr = 0;
			for(Collection<TableRow> rowCluster : getRowClusters(new ArrayList<>(clu))) {
				if(rowCluster.size()>2) {
					for(TableRow row : rowCluster) {
						sb.append(String.format("%d:\t[%3d]\t%s\n", cluNr, row.getTable().getTableId(), row.format(20)));
					}
					cluNr++;
				}
			}
			
			Map<Collection<TableColumn>, TableColumn> schemaClu = localSchemaClusters.createResult();
			sb.append(String.format("Schema Clusters for Cluster %d\n", idx));
	    	int schemaCluIdx = 0;
	    	for(Collection<TableColumn> cl : schemaClu.keySet()) {
	    		sb.append("\tCluster " + schemaCluIdx++ + ": ");
	    		
	    		ArrayList<String> labels = new ArrayList<>();
	    		
	    		for(TableColumn c : cl) {
	    			labels.add(String.format("{%d}[%d]%s", c.getTable().getTableId(), c.getColumnIndex(), c.getHeader()));
//	    			labels.add(String.format("\t{%d}[%d]%s(%s)", c.getTable().getTableId(), c.getColumnIndex(), c.getHeader(), StringUtils.join(Q.project(c.getProvenance(), new Func<String, String>() {
//
//						@Override
//						public String invoke(String in) {
//							String[] values = in.split(";");
//							return String.format("[%s]%s", values[1], values[2]);
//						}}), ",")));
	    		}
	    		
	    		Collections.sort(labels);
	    		
	    		sb.append("\t" + StringUtils.join(labels, ",") + "\n");
	    	}
			
//	    	sb.append(calculateFrequentColumnCombinations(clu));
	    	
			sb.append("\n");
			idx++;
		}
		
		Map<Collection<TableColumn>, TableColumn> schemaClu = schemaClusters.createResult();
		sb.append("Schema Clusters\n");
    	int schemaCluIdx = 0;
    	for(Collection<TableColumn> cl : schemaClu.keySet()) {
    		sb.append("Cluster " + schemaCluIdx++ + ": ");
    		
    		ArrayList<String> labels = new ArrayList<>();
    		
    		for(TableColumn c : cl) {
    			labels.add(String.format("[%d]%s", c.getColumnIndex(), c.getHeader()));
//    			labels.add(String.format("[%d]%s(%s)", c.getColumnIndex(), c.getHeader(), StringUtils.join(Q.project(c.getProvenance(), new Func<String, String>() {
//
//					@Override
//					public String invoke(String in) {
//						String[] values = in.split(";");
//						return String.format("[%s]%s", values[1], values[2]);
//					}}), ",")));
    		}
    		
    		Collections.sort(labels);
    		
    		sb.append("\t" + StringUtils.join(labels, ",") + "\n");
    	}
    	
    	// create the table clusters
    	Map<Collection<Table>, Table> tableClusters = tableComponents.createResult();
    	
    	sb.append("Table Clusters\n");
    	int tableCluIdx = 0;
    	for(Collection<Table> clu : tableClusters.keySet()) {
    		sb.append("Cluster " + tableCluIdx++ + ": ");
    		
    		ArrayList<String> labels = new ArrayList<>();
    		
    		for(Table t : clu) {
//    			labels.add(tableToSchema.get(t.getTableId()));
    			labels.add(StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), "+"));
    		}
    		
    		Collections.sort(labels);
    		
    		sb.append("\t" + StringUtils.join(labels, ",") + "\n");
    	}
	
    	// print links between clusters
    	List<Table> list = new ArrayList<>(allTables);
    	sb.append("Links between Clusters:\n");
    	for(int i = 0; i < list.size(); i++) {
    		Table t1 = list.get(i);
    		for(int j = i+1; j < list.size(); j++) {
    			Table t2 = list.get(j);
    			
    			int clu1 = tableToCluster.get(t1);
    			int clu2 = tableToCluster.get(t2);
    			
    			if(clu1!=clu2) {
    				
    				ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> cors = getSchemaCorrespondences(t1, t2);
    				
    				if(cors!=null) {
    				
    					sb.append(String.format("{%s}<->{%s}:\n", t1.getPath(), t2.getPath()));
    					sb.append(String.format("Keys for %s: %s\n", t1.getPath(), StringUtils.join(Q.project(t1.getSchema().getCandidateKeys(), new Func<String,Collection<TableColumn>>() {

							@Override
							public String invoke(Collection<TableColumn> in) {
								return String.format("{%s}", StringUtils.join(Q.project(in, new TableColumn.ColumnHeaderProjection()), ","));
							}}), ",")));
    					sb.append(String.format("Keys for %s: %s\n", t2.getPath(), StringUtils.join(Q.project(t2.getSchema().getCandidateKeys(), new Func<String,Collection<TableColumn>>() {

							@Override
							public String invoke(Collection<TableColumn> in) {
								return String.format("{%s}", StringUtils.join(Q.project(in, new TableColumn.ColumnHeaderProjection()), ","));
							}}), ",")));
    					
    					for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : cors.get()) {
    						TableColumn c1 = t1.getSchema().get(cor.getFirstRecord().getColumnIndex());
    						TableColumn c2 = t2.getSchema().get(cor.getSecondRecord().getColumnIndex());
    						sb.append(String.format("   (%.8f) [%d] %s <-> [%d] %s\n", cor.getSimilarityScore(), c1.getColumnIndex(), c1.getHeader(), c2.getColumnIndex(), c2.getHeader()));
    					}
    				}
    				
    			}
    		}
    	}
    	
    	Set<Collection<String>> synonyms = getAttributeNameClusters(new ArrayList<>(allTables));
    	sb.append("Synonym sets\n");
    	for(Collection<String> s : synonyms) {
    		sb.append(String.format("\t%s\n", StringUtils.join(s, ",")));
    	}
    	
    	
		return sb.toString();
	}
	
	public Set<Collection<TableColumn>> getAttributeClusters(List<Table> tables) {
		ConnectedComponentClusterer<TableColumn> schemaClusterer = new ConnectedComponentClusterer<>();
		
		// create the schema clusters
		for(int i = 0; i < tables.size(); i++) {
			Table t1 = tables.get(i);
			
			for(int j = i+1; j < tables.size(); j++) {
				Table t2 = tables.get(j);

				Table tt1 = t1;
				Table tt2 = t2;
				ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = getSchemaCorrespondences(t1, t2);
				if(schemaCorrespondences==null) {
					tt1 = t2;
					tt2 = t1;
					schemaCorrespondences = getSchemaCorrespondences(tt1, tt2);
				}
				if(schemaCorrespondences!=null) {
					for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
						TableColumn c1;
						TableColumn c2;
						if(t1==tt1) {
							c1 = tt1.getSchema().get(cor.getFirstRecord().getColumnIndex());
							c2 = tt2.getSchema().get(cor.getSecondRecord().getColumnIndex());
						} else {
							c2 = tt1.getSchema().get(cor.getFirstRecord().getColumnIndex());
							c1 = tt2.getSchema().get(cor.getSecondRecord().getColumnIndex());
						}
						
						// add the correspondences to the graph for schema clustering
						schemaClusterer.addEdge(new Triple<TableColumn, TableColumn, Double>(c1, c2, cor.getSimilarityScore()));
					}
				}
			}
		}
		
		// create the schema clusters
		return schemaClusterer.createResult().keySet();
	}
	
	public Set<Collection<String>> getAttributeNameClusters(List<Table> tables) {
		ConnectedComponentClusterer<String> schemaClusterer = new ConnectedComponentClusterer<>();
		
		// create the schema clusters
		for(int i = 0; i < tables.size(); i++) {
			Table t1 = tables.get(i);
			
			for(TableColumn c : t1.getColumns()) {
				schemaClusterer.addEdge(new Triple<String, String, Double>(c.getHeader(), c.getHeader(), 1.0));
			}
			
			for(int j = i+1; j < tables.size(); j++) {
				Table t2 = tables.get(j);
				
				Table tt1 = t1;
				Table tt2 = t2;
				ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = getSchemaCorrespondences(t1, t2);
				if(schemaCorrespondences==null) {
					tt1 = t2;
					tt2 = t1;
					schemaCorrespondences = getSchemaCorrespondences(tt1, tt2);
				}
				if(schemaCorrespondences!=null) {
					for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
						TableColumn c1;
						TableColumn c2;
						if(t1==tt1) {
							c1 = tt1.getSchema().get(cor.getFirstRecord().getColumnIndex());
							c2 = tt2.getSchema().get(cor.getSecondRecord().getColumnIndex());
						} else {
							c2 = tt1.getSchema().get(cor.getFirstRecord().getColumnIndex());
							c1 = tt2.getSchema().get(cor.getSecondRecord().getColumnIndex());
						}
						
						// add the correspondences to the graph for schema clustering
						schemaClusterer.addEdge(new Triple<String, String, Double>(c1.getHeader(), c2.getHeader(), cor.getSimilarityScore()));
					}
				}
			}
		}
		
		// create the schema clusters
		return schemaClusterer.createResult().keySet();
	}
	
	public Set<Collection<TableRow>> getRowClusters(List<Table> tables) {
		ConnectedComponentClusterer<TableRow> rowClusterer = new ConnectedComponentClusterer<>();
		
		// create the schema clusters
		for(int i = 0; i < tables.size(); i++) {
			Table t1 = tables.get(i);
			
			for(int j = i+1; j < tables.size(); j++) {
				Table t2 = tables.get(j);
				ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = getInstanceCorrespondences(t1, t2);
				
				if(instanceCorrespondences!=null) {
					
					for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : instanceCorrespondences.get()) {
						rowClusterer.addEdge(new Triple<TableRow, TableRow, Double>(t1.get(cor.getFirstRecord().getRowNumber()), t2.get(cor.getSecondRecord().getRowNumber()), cor.getSimilarityScore()));
					}
					
				}
			}
		}
		
		// create the schema clusters
		return rowClusterer.createResult().keySet();
	}
	
	public int countSchemaCorrespondences() {
		int count = 0;
		
		for(Table t1 : schemaCorrespondences.keySet()) {
			for(Table t2 : schemaCorrespondences.get(t1).keySet()) {
				count += schemaCorrespondences.get(t1).get(t2).size();
			}
		}
		
		return count;
	}
	
	public Map<String, Collection<String>> getAttributeNameClusterIndex(Collection<Table> tables) {
		ArrayList<Table> list = new ArrayList<>(tables);
		Set<Collection<String>> attributeNameClusters = getAttributeNameClusters(list);
		Map<String, Collection<String>> attributeNameIndex = new HashMap<>();
		
//    	System.out.println("Attribute Name Clusters");
//    	int idx = 0;
//    	for(Collection<String> clu : attributeNameClusters) {
//    		System.out.println("Cluster " + idx++ + ": ");
//    		
//    		ArrayList<String> labels = new ArrayList<>(clu);
//    		
//    		Collections.sort(labels);
//    		
//    		System.out.println("\t" + StringUtils.join(labels, ","));
//    	}
		
		// filter attribute names: remove empty names
		// and create an index of attribute names
		for(Collection<String> clu : attributeNameClusters) {
			Iterator<String> it = clu.iterator();
			
			while(it.hasNext()) {
				String attributeName = it.next();
				
				if(attributeName.equals("null")) {
					it.remove();
				} else {
					attributeNameIndex.put(attributeName, clu);
				}
			}
		}
		
		return attributeNameIndex;
	}
	
	public int addSchemaCorrespondencesViaAttributeNames(Collection<Table> tables) {
		int changeCount = 0;
		ArrayList<Table> list = new ArrayList<>(tables);
		
		Map<String, Collection<String>> attributeNameIndex = getAttributeNameClusterIndex(tables);
		
		// iterate over all pairs of tables and add correspondences based on attribute names
		for(int i = 0; i < list.size(); i++) {
			Table t1 = list.get(i);
			for(int j = i+1; j < list.size(); j++) {
				Table t2 = list.get(j);
				
				// iterate over all pairs of columns for the current pair of tables
				for(TableColumn c1 : t1.getColumns()) {
					if(!SpecialColumns.isSpecialColumn(c1)) { // no need to match the 'source table' column or 'row number' column
						Collection<String> cluster = attributeNameIndex.get(c1.getHeader());
						
						if(cluster!=null) {
							for(TableColumn c2 : t2.getColumns()) {
								if(c2.getColumnIndex()!=0) {
									Collection<String> cluster2 = attributeNameIndex.get(c2.getHeader());
									
									if(cluster.equals(cluster2)) {
										Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(new MatchableTableColumn(t1.getTableId(), c1), new MatchableTableColumn(t2.getTableId(),  c2), 1.0, null);
										if(addSchemaCorrespondence(t1, t2, cor)) {
											changeCount++;
											System.out.println(String.format("Adding {%d}[%d]%s<->{%d}[%d]%s", t1.getTableId(), c1.getColumnIndex(), c1.getHeader(), t2.getTableId(), c2.getColumnIndex(), c2.getHeader()));
										}
									}
								}
							}
						}
					}
				}
			}
		}
		
		// update disjoint headers: add all synonyms to the disjoint headers
		HashMap<String, Set<String>> newDisjointHeaders = new HashMap<>();
		for(String header : disjointHeaders.keySet()) {
			
			Set<String> disjoint = new HashSet<>();
			
			for(String old : disjointHeaders.get(header)) {
				// for each existing header
				disjoint.add(old);
				
				// add all synonyms
				disjoint.addAll(attributeNameIndex.get(old));
			}
			
			// add the new disjoint headers for all synonyms of the current header
			newDisjointHeaders.put(header, disjoint);
			for(String synonym : attributeNameIndex.get(header)) {
				newDisjointHeaders.put(synonym, disjoint);
			}
		}
		disjointHeaders = newDisjointHeaders;
		
		return changeCount;
	}
	
	public void addSchemaCorrespondencesViaColumnPositions() {
		ColumnOrderInference coi = new ColumnOrderInference();
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> newCorrespondences = coi.inferCorrespondencesByColumnOrder(allTables, getAttributeClusters(new ArrayList<>(allTables)));
		
		Map<Integer, Table> tablesById = new HashMap<>();
		for(Table t: allTables) {
			tablesById.put(t.getTableId(), t);
		}
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : newCorrespondences.get()) {
			Table t1 = tablesById.get(cor.getFirstRecord().getTableId());
			Table t2 = tablesById.get(cor.getSecondRecord().getTableId());
			
			addSchemaCorrespondence(t1, t2, cor);
		}
		
	}
	
	/**
	 * 
	 * @param tables
	 * @return the number of added correspondences
	 */
	public int materialiseSchemaCorrespondenceTransitivity() {
		
		boolean hasChanges = false;
		int changeCount = 0;
		
		// index tables by id
		Map<Integer, Table> tableIndex = new HashMap<>();
		for(Table t : allTables) {
			tableIndex.put(t.getTableId(), t);
		}
		
		int round = 1;
		
		do {
			System.out.println(String.format("Round %d", round++));
			hasChanges = false;
			
			// iterate over all tables
			for(Table t : allTables) {
				
				int addCount = 0;
				
				Map<TableColumn, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>> correspondences = getSchemaCorrespondecesPerAttribute(t);
				
				// for all attributes with more than one correspondence to another table
				for(TableColumn col : correspondences.keySet()) {
					ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> cors = correspondences.get(col);
					
					if(cors.size()>1) {
						
						// assume no two correspondences point to the same table ...
						ArrayList<Correspondence<MatchableTableColumn, MatchableTableRow>> corList = new ArrayList<>(cors.get());
						
						// for all pairs of tables that are being connected to
						for(int i = 0; i < corList.size(); i++) {
							
							Correspondence<MatchableTableColumn, MatchableTableRow> cor1 = corList.get(i);
							
							for(int j = i+1; j < corList.size(); j++) {

								Correspondence<MatchableTableColumn, MatchableTableRow> cor2 = corList.get(j);
								
								// add a schema correspondence between the tables in the pair for the attribute, if it does not already exist
								Correspondence<MatchableTableColumn, MatchableTableRow> newCor = new Correspondence<MatchableTableColumn, MatchableTableRow>(cor1.getSecondRecord(), cor2.getSecondRecord(), cor1.getSimilarityScore() * cor2.getSimilarityScore(), null);

								Table t1 = tableIndex.get(cor1.getSecondRecord().getTableId());
								Table t2 = tableIndex.get(cor2.getSecondRecord().getTableId());
								
								if(addSchemaCorrespondence(t1, t2, newCor)) {
									hasChanges = true;
									addCount++;
									
									TableColumn col1 = t1.getSchema().get(cor1.getSecondRecord().getColumnIndex());
									TableColumn col2 = t2.getSchema().get(cor2.getSecondRecord().getColumnIndex());
									
									changeCount++;
									System.out.println(String.format("Adding {%d}[%d]%s<->{%d}[%d]%s via {%d}[%d]%s", t1.getTableId(), col1.getColumnIndex(), col1.getHeader(), t2.getTableId(), col2.getColumnIndex(), col2.getHeader(), t.getTableId(), col.getColumnIndex(), col.getHeader()));
								}
								
								
							}
						}
					}
				}
				
				if(addCount>0) 
					System.out.println(String.format("Table %d: %d correspondences added.", t.getTableId(), addCount));
			}
				
		} while(hasChanges); // until no more edges are added
		
		return changeCount;
	}
	
	/**
	 * 
	 * if a matching key holds between t and t1 as well as between t and t2, then this method will create this matching key between t1 and t2
	 * 
	 * @return
	 */
	public int materialiseMatchingKeyTransitivity() {
		boolean hasChanges = false;
		int changeCount = 0;
		
		// index tables by id
		Map<Integer, Table> tableIndex = new HashMap<>();
		for(Table t : allTables) {
			tableIndex.put(t.getTableId(), t);
		}
		
		int round = 1;
		
		do {
			System.out.println(String.format("Round %d", round++));
			hasChanges = false;
			
			// iterate over all tables
			for(Table t : getTablesWithKeyCorrespondences()) {
				
				int addCount = 0;
				
				List<Table> connections = getTablesConnectedViaKeyCorrespondences(t);
				
				// if there are at least two connections to other tables
				if(connections.size()>1) {
		
					// for all pairs of tables that are being connected to
					for(int i = 0; i < connections.size(); i++) {
						for(int j = i+1; j < connections.size(); j++) {
							
							// assume that schema correspondences have been materialised before, so we can assume that exist
							
							Table t1 = connections.get(i);
							Table t2 = connections.get(j);
							
							// get all matching keys that exists for t<->t1 and t<->t2
							Collection<WebTableMatchingKey> keysToT1 = getMatchingKeys(t, t1);
							Collection<WebTableMatchingKey> keysToT2 = getMatchingKeys(t, t2);
							Collection<WebTableMatchingKey> keys = new LinkedList<>();
							for(WebTableMatchingKey k1 : keysToT1) {
								for(WebTableMatchingKey k2 : keysToT2) {
									if(k1.getFirst().equals(k2.getFirst())) {
										// this key has the same columns for t, so it is the same matching key for t1 and t2
										// translate it into a matching key between t1 and t2
										keys.add(new WebTableMatchingKey(k1.getSecond(), k2.getSecond()));
									}
								}
							}
							
							// check all keys that exist between t and both t1 and t2
							for(WebTableMatchingKey k : keys) {
								
								// check if it exists between t1 and t2
								Set<WebTableMatchingKey> keys12 = getMatchingKeys(t1, t2);
								
								if(keys12==null) {
									keys12 = new HashSet<>();
								}
								
								// make sure that no existing matching key contains the current one (k)
								boolean betterKeyExists = false;
								for(WebTableMatchingKey ck : keys12) {
									if(P.CollectionIsContainedInValues(k.getFirst(),ck.getFirst())
											&& P.CollectionIsContainedInValues(k.getSecond(),ck.getSecond())) {
										betterKeyExists=true;
										break;
									}
								}
								
								
								if(!keys12.contains(k) && !betterKeyExists) {
									
									// if not, add it and remove all matching keys that are now subsumed by the new one
									Collection<WebTableMatchingKey> replacedKeys = new LinkedList<>();
									for(WebTableMatchingKey ck : keys12) {
										if(P.CollectionIsContainedInValues(ck.getFirst(), k.getFirst())
												&& P.CollectionIsContainedInValues(ck.getSecond(), k.getSecond())) {
											replacedKeys.add(ck);
										}
									}
									
									removeMatchingKeys(t1, t2);
									keys12.removeAll(replacedKeys);
									keys12.add(k);
									addMatchingKeys(t1, t2, keys12);
									hasChanges=true;
									addCount++;
									
									// update candidate keys
									Iterator<Set<TableColumn>> it = t1.getSchema().getCandidateKeys().iterator(); 
									while(it.hasNext()) {
										Collection<TableColumn> key = it.next();
										
										if(k.getFirst().containsAll(key)) {
											// the new key contains this old key
											it.remove();
										}
									}
									it = t2.getSchema().getCandidateKeys().iterator(); 
									while(it.hasNext()) {
										Collection<TableColumn> key = it.next();
										
										if(k.getSecond().containsAll(key)) {
											// the new key contains this old key
											it.remove();
										}
									}
									t1.getSchema().getCandidateKeys().add(k.getFirst());
									t2.getSchema().getCandidateKeys().add(k.getSecond());
									
									System.out.println(String.format("Added matching key [%d]{%s}<->[%d]{%s}", 
											t1.getTableId(),
											StringUtils.join(Q.project(k.getFirst(), new TableColumn.ColumnHeaderProjection()), ","),
											t2.getTableId(),
											StringUtils.join(Q.project(k.getSecond(), new TableColumn.ColumnHeaderProjection()), ",")
											));
									for(WebTableMatchingKey ck : replacedKeys) {
										System.out.println(String.format("  Replaced matching key [%d]{%s}<->[%d]{%s}", 
												t1.getTableId(),
												StringUtils.join(Q.project(ck.getFirst(), new TableColumn.ColumnHeaderProjection()), ","),
												t2.getTableId(),
												StringUtils.join(Q.project(ck.getSecond(), new TableColumn.ColumnHeaderProjection()), ",")
												));
									}
								}
								
							}
							
							
						}
						
					}

				}
				
				if(addCount>0) 
					System.out.println(String.format("Table %d: %d matching keys added.", t.getTableId(), addCount));
			}
				
		} while(hasChanges); // until no more edges are added
		
		return changeCount;
	}
	
	/**
	 * 
	 * if t1 has a key with matching columns in t2, then this key will be added to t2 (if it is not contained in an already existing key)
	 * 
	 * @param tables
	 */
	public int propagateMatchingKeys() {
		boolean hasChanges = false;
		int changeCount = 0;
		
		// index tables by id
		Map<Integer, Table> tableIndex = new HashMap<>();
		for(Table t : allTables) {
			tableIndex.put(t.getTableId(), t);
		}
		
		int round = 1;
		
		MatchingKeyGenerator mkg = new MatchingKeyGenerator();
		
		do {
			System.out.println(String.format("Round %d", round++));
			hasChanges = false;
			
			// iterate over all tables
			for(Table t : getTablesWithSchemaCorrespondences()) {
				
				List<Table> connections = getTablesConnectedViaSchemaCorrespondences(t);
				
				Set<String> watch1 = new HashSet<>(Arrays.asList(new String[] { "5550.json" }));
				if(watch1.contains(t.getPath())) {
					System.out.println("test");
				}
				
				// for each directly connected table (via a schema correspondence)
				for(Table t2 : connections) {
		
					Set<WebTableMatchingKey> oldKeys = getMatchingKeys(t, t2);
					
					Set<String> watch = new HashSet<>(Arrays.asList(new String[] { "5550.json" }));
					if(watch.contains(t.getPath()) && watch.contains(t2.getPath())) {
						System.out.println("test");
					}
					
					Set<WebTableMatchingKey> newKeys12 = mkg.generateAllJoinKeysFromCorrespondences(t, t2, getSchemaCorrespondences(t, t2), 1.0);
					Set<WebTableMatchingKey> newKeys21 = mkg.generateAllJoinKeysFromCorrespondences(t2, t, getSchemaCorrespondences(t2, t), 1.0);
					
					// generate keys for both combinations (results may differ) and merge them to get the complete set of matching keys
					Set<WebTableMatchingKey> newKeys = new HashSet<>();
					if(newKeys12!=null) {
						newKeys.addAll(newKeys12);
					}
					if(newKeys21!=null) {
						for(WebTableMatchingKey k : newKeys21) {
							newKeys.add(k.invert());
						}
					}
					
					removeMatchingKeys(t, t2);
					
					// this version will add empty matching keys, so all tables will be merged
//					if(newKeys!=null) {
//						addMatchingKeys(t, t2, newKeys);
//					}
					
					// this is the correct version:
					if(newKeys!=null && newKeys.size()>0) {
						addMatchingKeys(t, t2, newKeys);
					} else {
						newKeys = null;
					}

					if((oldKeys==null&&newKeys!=null) || (oldKeys!=null&&newKeys!=null&&!oldKeys.equals(newKeys))) {
						hasChanges=true;
						
						for(WebTableMatchingKey key : Q.without(newKeys, oldKeys)) {
							System.out.println(String.format("~~~~key: {%s}{%s}<->{%s}{%s}", t.getPath(), Q.project(key.getFirst(), new TableColumn.ColumnHeaderProjection()),
									t2.getPath(), Q.project(key.getSecond(), new TableColumn.ColumnHeaderProjection())));
						}
					}
				}

			}
				
		} while(hasChanges); // until no more edges are added
		
		return changeCount;
	}
	
	public void checkCandidateKeyUniqueness() {
		
		for(Table t : allTables) {
			
			Iterator<Set<TableColumn>> keyIt = t.getSchema().getCandidateKeys().iterator();
			
			while(keyIt.hasNext()) {
				
				Set<TableColumn> key = keyIt.next();
				
				HashSet<String> uniqueValues = new HashSet<>();
				
				int lastSize = uniqueValues.size();
				
				for(TableRow r : t.getRows()) {
					
					String keyValue = StringUtils.join(r.project(key), "");
					
					uniqueValues.add(keyValue);
					
					if(uniqueValues.size()==lastSize) {
						// the value did already exist
						System.out.println(String.format("Removed Key {%s}, not unique!", Q.project(key, new TableColumn.ColumnHeaderProjection())));
						keyIt.remove();
						break;
					}
					
					lastSize = uniqueValues.size();
					
				}
				 
			}
			
		}
		
	}
	
	public void removeInvalidCandidateKeys() {
		
		// remove invalid candidate keys
		// keys which could be matching keys (all columns matched), but cannot be found as keys in other tables, are removed
//		
		for(Table t : allTables) {
			List<Table> connected = getTablesConnectedViaKeyCorrespondences(t);
			if(connected!=null) {
				for(Table t2 : connected) {
					MatchingKeyGenerator mkg = new MatchingKeyGenerator();
					
					mkg.removeInvalidCandidateKeys(t, t2, getSchemaCorrespondences(t, t2), 0.0);
				}
			}
		}
		
		// keys which are no matching keys will have no values in the fused table, hence they cannot be keys! (CWA)
		// so, remove all keys which are not matching keys
		
		
		for(Table t : allTables) {
			
			Map<Table, Set<WebTableMatchingKey>> matchingKeysForT = matchingKeys.get(t);
			
			if(matchingKeysForT!=null) {
				Set<Set<TableColumn>> allKeysUsedAsMatchingKey = new HashSet<>();
				for(Set<WebTableMatchingKey> keysWithT2 : matchingKeysForT.values()) {
					for(WebTableMatchingKey mk : keysWithT2) {
						allKeysUsedAsMatchingKey.add(mk.getFirst());
					}
				}
				
				Iterator<Set<TableColumn>> keyIt = t.getSchema().getCandidateKeys().iterator();
				while(keyIt.hasNext()) {
					Set<TableColumn> key = keyIt.next();
				
					if(!allKeysUsedAsMatchingKey.contains(key)) {
						keyIt.remove();
						System.out.println(String.format("Removed Key {%s} (no matching key found)", Q.project(key, new TableColumn.ColumnHeaderProjection())));
					}
				}
			
			}
			
		}
		
	}
	
	public Set<Collection<Table>> getConnectedTables() {
		ConnectedComponentClusterer<Table> con = new ConnectedComponentClusterer<>();
		
		//TODO it can make quite a difference whether schema or instance correspondences are used here!
		// already the first matching phase can result in tables being connected by schema correspondences without any remaining instance correspondences!
		// the second matching round will very likely result in such a scenario
		// solution: use connection via matching keys?
		// - makes sure that only those tables are connected, where instances can be merged
		// - should take special care of partially matching keys in a separate step
//		for(Table t : getTablesWithKeyCorrespondences()) {
		for(Table t : allTables) {
			List<Table> connected = getTablesConnectedViaKeyCorrespondences(t);
			
			// add self-edge to make sure that all tables (even if not connected) end up in a cluster
			con.addEdge(new Triple<Table, Table, Double>(t, t, 1.0));
			
			if(connected!=null) {
				for(Table t2 : connected) {
					con.addEdge(new Triple<Table, Table, Double>(t, t2, 1.0));
				}
			}
		}
		
		// merge tables only if all attributes that were part of a key in the original table are still part of a key in the combined table?
		// - initial instance correspondences based on matching keys
		// - at least one key must be matched
		// - attributes which are not in the matched key, but are part of another key, must be matched, otherwise, the resulting table would need a larger key (and by the the keys used for matching become invalid?)
		
		return con.createResult().keySet();
	}
	
	public WebTablesMatchingGraph fuseTables(int firstTableId) {
		// use the table and schema clusters to fuse all tables at once
		
		// for each table cluster
		//  for each schema cluster
		//   map all attributes to the same output attribute
		
		SchemaFuser fuser = new SchemaFuser();
		int tableId = firstTableId;
		LinkedList<Table> tables = new LinkedList<>();
		
		Set<Collection<Table>> clusters = getConnectedTables();
		Map<Collection<Table>, Table> fusedTablesPerCluster = new HashMap<>();
		Map<Table, Table> tableToFusedTableMap = new HashMap<>();
		// a map that translates all original columns to their columns in the fused tables
		Map<TableColumn,TableColumn> columnMapping = new HashMap<>();
		
		for(Collection<Table> tableCluster : clusters) {
			Table table = new Table();
			table.setPath(Integer.toString(tableId) + "_"  + tableCluster.size());
			table.setTableId(tableId);
			fusedTablesPerCluster.put(tableCluster, table);
			
			// build a map that translates all original tables to their fused table
			for(Table t : tableCluster) {
				tableToFusedTableMap.put(t, table);
			}
			
			/*********************************
			 *  Column Clusters
			 *********************************/
			// get the attribute clusters
			Set<Collection<TableColumn>> schemaClusters = getAttributeClusters(new ArrayList<>(tableCluster));

    		// add a column with the table id (context surrogate key) as first column in every table
			TableColumn tableIdColumn = new TableColumn(0, table);
			tableIdColumn.setDataType(DataType.string);
			tableIdColumn.setHeader(SpecialColumns.SOURCE_TABLE_COLUMN);
			table.insertColumn(0, tableIdColumn);
			TableColumn rowNumberColumn = new TableColumn(1, table);
			rowNumberColumn.setDataType(DataType.string);
			rowNumberColumn.setHeader(SpecialColumns.ROW_NUMBER_COLUMN);
			table.insertColumn(1, rowNumberColumn);
			
			int columnIndex = 2;
			
			// map all columns in the cluster to a new column in the fused table
			for(Collection<TableColumn> schemaCluster : schemaClusters) {
				TableColumn c = new TableColumn(columnIndex++, table);
				Distribution<String> headers = Distribution.fromCollection(schemaCluster, new TableColumn.ColumnHeaderProjection());
//				String newHeader = String.format("%s {%s}", headers.getMode(), StringUtils.join(Q.project(schemaCluster, new TableColumn.ColumnIdentifierProjection()), ";"));
				String newHeader = headers.getMode();
				c.setHeader(newHeader);
				
				// add provenance for column
				for(TableColumn c0 : schemaCluster) {
					c.addProvenanceForColumn(c0);
				}
				
				table.addColumn(c);
				
				for(TableColumn col : schemaCluster) {
					columnMapping.put(col, c);
				}
			}
			
			// add the remaining mappings and columns to the new table
			for(Table t : tableCluster) {
				// add the mapping for the source table column
				columnMapping.put(t.getSchema().get(0), table.getSchema().get(0));
				// add the mapping for the row number column
				columnMapping.put(t.getSchema().get(1), table.getSchema().get(1));
				
				// add all unmapped columns to the table
				for(TableColumn c : t.getColumns()) {
					if(!columnMapping.containsKey(c)) {
						TableColumn colNew = c.copy(table, table.getColumns().size());
//						colNew.setHeader(String.format("%s {%s}", colNew.getHeader(), c.getIdentifier()));
						table.addColumn(colNew);
						columnMapping.put(c, colNew);
					}
				}
			}
			
			for(Table t : tableCluster) {
				// add the dependencies to the new table
				for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
					Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
					
					// translate the dependencies via columnMapping
					Collection<TableColumn> det2 = new ArrayList<>(det.size());
					Collection<TableColumn> dep2 = new ArrayList<>(dep.size());
					
					for(TableColumn c : det) {
						det2.add(columnMapping.get(c));
					}
					for(TableColumn c : dep) {
						dep2.add(columnMapping.get(c));
					}
					
					table.getSchema().getFunctionalDependencies().put(det2, dep2);
				}
				
				// add the candidate keys to the new table
				for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {
					Set<TableColumn> fusedKey = new HashSet<>(key.size());
					
					for(TableColumn col : key) {
						fusedKey.add(columnMapping.get(col));
					}
					
					table.getSchema().getCandidateKeys().add(fusedKey);
				}
			}
			
			/*********************************
			 *  Row Clusters
			 *********************************/
			// get the instance clusters
			Set<Collection<TableRow>> rowClusters = getRowClusters(new ArrayList<>(tableCluster));
			
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = new ResultSet<>();
			
			int rowNumber = 0;
			
			// map all rows with correspondences to a new row in the fused table
			for(Collection<TableRow> rowCluster : rowClusters) {
				
				if(rowCluster.size()==0) {
					System.out.println("Empty Row Cluster!");
				}
				
				TableRow r = new TableRow(rowNumber++, table);
//				r.set(new Object[] { "UNMERGED: " + StringUtils.join(Q.project(rowCluster, new TableRow.TableRowIdentifierProjection()), ";") } );
				r.set(new Object[0]);
				
				// add provenance for row
				for(TableRow r0 : rowCluster) {
					r.addProvenanceForRow(r0);
				}
				
				table.addRow(r);
				
				for(TableRow row : rowCluster) {
					instanceCorrespondences.add(
							new Correspondence<MatchableTableRow, MatchableTableColumn>(
									new MatchableTableRow(row, row.getTable().getTableId()), 
									new MatchableTableRow(r, tableId), 
									1.0,
									null));
				}
			}

			/*********************************
			 *  Table Fusion
			 *********************************/
			//fuse the rows of all tables in the current table cluster
			for(Table t : tableCluster) {
				fuser.mergeRows(t, table, columnMapping, instanceCorrespondences);
			}
			
			tables.add(table);
			
			tableId++;

			//TODO merge all candidate keys, dependencies, context-dependent flag or propagate them in the graph before such that we can copy them from any table
			// merge the candidate keys
//			HashSet<Set<TableColumn>> candidateKeys = new HashSet<>();
//			for(Table t : tableCluster) {
//				for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {
//					Set<TableColumn> fusedKey = new HashSet<>(key.size());
//					
//					for(TableColumn col : key) {
//						fusedKey.add(columnMapping.get(col));
//					}
//					
//					candidateKeys.add(fusedKey);
//				}
//			}
//			table.getSchema().setCandidateKeys(candidateKeys);
		
			
			System.out.println(String.format("Merged Tables: {%s}=>%s", StringUtils.join(Q.project(tableCluster, new Table.TablePathProjection()), ","), table.getPath()));
		}
		
		// transform this matching graph into a matching graph for the fused tables
		WebTablesMatchingGraph fusedGraph = new WebTablesMatchingGraph(tables);
		
		// iterate over all schema correspondences
		for(Table t1 : getTablesWithSchemaCorrespondences()) {
			Table fusedT1 = tableToFusedTableMap.get(t1);
			for(Table t2 : getTablesConnectedViaSchemaCorrespondences(t1)) {
				Table fusedT2 = tableToFusedTableMap.get(t2);
				// check if the correspondence holds between two different fused tables
				if(!fusedT1.equals(fusedT2)) {
					
					ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> fusedCorrespondences = new ResultSet<>();
					
					// if so, translate the correspondences to the new tables
					for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : getSchemaCorrespondences(t1, t2).get()) {
						TableColumn col1 = t1.getSchema().get(cor.getFirstRecord().getColumnIndex());
						TableColumn col2 = t2.getSchema().get(cor.getSecondRecord().getColumnIndex());
						
						TableColumn fusedCol1 = columnMapping.get(col1);
						TableColumn fusedCol2 = columnMapping.get(col2);
						
						Correspondence<MatchableTableColumn, MatchableTableRow> newCor 
						= new Correspondence<MatchableTableColumn, MatchableTableRow>(
								new MatchableTableColumn(fusedT1.getTableId(), fusedCol1), 
								new MatchableTableColumn(fusedT2.getTableId(), fusedCol2), 
								cor.getSimilarityScore(), 
								null);
					
						fusedCorrespondences.add(newCor);
					}
					
					fusedCorrespondences.deduplicate();
					fusedGraph.addSchemaCorrespondences(fusedT1, fusedT2, fusedCorrespondences);
				}
			}
		}
		// no need to iterate over join keys or instance correspondences as they cannot exist between different fused tables (otherwise we would have merged them)
		
		return fusedGraph;
	}
	
	public void writeMatchingKeyGraph(File f) throws IOException {
		Graph<Table, WebTableMatchingKey> g = new Graph<>();
		
		for(Table t : Q.sort(allTables, new Table.TablePathComparator())) {
			g.addNode(t);
		}
		
		for(Table t1 : getTablesWithKeyCorrespondences()) {
			for(Table t2 : getTablesConnectedViaKeyCorrespondences(t1)) {
				Collection<WebTableMatchingKey> keys = getMatchingKeys(t1, t2);
				
				for(WebTableMatchingKey k : keys) {
	
					g.addEdge(t1, t2, k, k.getFirst().size());
				
				}
			}
		}
		
		g.writePajekFormat(f);
	}
	
	public void writeInstanceCorrespondenceGraph(File f) throws IOException {
		Graph<Table, Object> g = new Graph<>();
		
		for(Table t : Q.sort(allTables, new Table.TablePathComparator())) {
			g.addNode(t);
		}
		
		for(Table t1 : getTablesWithInstanceCorrespondences()) {
			for(Table t2 : getTablesConnectedViaInstanceCorrespondences(t1)) {
	
				int numCors = getInstanceCorrespondences(t1, t2).size();
				g.addEdge(t1, t2, String.format("%d correspondences", numCors), numCors);
				
			}
		}
		
		g.writePajekFormat(f);
	}
	
	public void writeSchemaCorrespondenceGraphByTable(File f) throws IOException {
		Graph<Table, Object> g = new Graph<>();
		HashMap<Integer, Table> index = new HashMap<>();
		for(Table t : Q.sort(allTables, new Table.TablePathComparator())) {
			g.addNode(t);
			index.put(t.getTableId(), t);
		}
		
		for(Table t1 : getTablesWithSchemaCorrespondences()) {
			for(Table t2 : getTablesConnectedViaSchemaCorrespondences(t1)) {
	
				g.addEdge(t1, t2, getSchemaCorrespondences(t1, t2), getSchemaCorrespondences(t1, t2).size());
//				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : getSchemaCorrespondences(t1, t2).get()) {
//					g.addEdge(index.get(cor.getFirstRecord().getTableId()), index.get(cor.getSecondRecord().getTableId()), cor, cor.getSimilarityScore());
//				}
				
			}
		}
		
		g.writePajekFormat(f);
	}
	
	public void writeSchemaCorrespondenceGraph(File f, DataSet<MatchableTableColumn, MatchableTableColumn> allAttributes) throws IOException {
		Graph<MatchableTableColumn, Object> g = new Graph<>();
		
		for(MatchableTableColumn c : allAttributes.get()) {
			if(!SpecialColumns.isSpecialColumn(c)) {
				g.addNode(c);
			}
		}
		
		for(Table t1 : getTablesWithSchemaCorrespondences()) {
			for(Table t2 : getTablesConnectedViaSchemaCorrespondences(t1)) {
	
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : getSchemaCorrespondences(t1, t2).get()) {
					g.addEdge(cor.getFirstRecord(), cor.getSecondRecord(), cor, cor.getSimilarityScore());
				}
				
			}
		}
		
		g.writePajekFormat(f);
	}
	
	public void writeSchemaCorrespondenceGraphForMatchingKeys(File f, DataSet<MatchableTableColumn, MatchableTableColumn> allAttributes) throws IOException {
		Graph<MatchableTableColumn, Object> g = new Graph<>();
		
		for(MatchableTableColumn c : allAttributes.get()) {
			if(!SpecialColumns.isSpecialColumn(c)) {
				g.addNode(c);
			}
		}
		
		for(Table t1 : getTablesWithSchemaCorrespondences()) {
			for(Table t2 : getTablesConnectedViaSchemaCorrespondences(t1)) {
	
				if(getMatchingKeys(t1, t2)!=null) {
				
					for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : getSchemaCorrespondences(t1, t2).get()) {
						g.addEdge(cor.getFirstRecord(), cor.getSecondRecord(), cor, cor.getSimilarityScore());
					}
				
				}
				
			}
		}
		
		g.writePajekFormat(f);
	}
}
