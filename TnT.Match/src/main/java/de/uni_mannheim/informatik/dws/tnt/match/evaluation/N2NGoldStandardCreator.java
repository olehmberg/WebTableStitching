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
package de.uni_mannheim.informatik.dws.tnt.match.evaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class N2NGoldStandardCreator {

	public void createFromMappedUnionTables(Collection<Table> allTables, File evaluationLocation) throws IOException {
		File union_tables_mapping = new File(evaluationLocation, "union_tables_mapping.tsv");
		File inter_union_mapping = new File(evaluationLocation, "inter_union_mapping.tsv");
		
		if(union_tables_mapping.exists() && inter_union_mapping.exists()) {
			// create a gold standard from a manual mapping of the union tables
			N2NGoldStandard gold = new N2NGoldStandard();
			N2NGoldStandard unionGold = new N2NGoldStandard();
			
			// map table -> column -> provenance
			HashMap<String, Map<Integer, Set<String>>> provenanceMapping = new HashMap<>();
			// a list of all clusters that will be in the final gold standard
			List<Set<String>> finalClusters = new LinkedList<>();
			Map<Set<String>, String> finalClusterNames = new HashMap<>();
			
			// read the mapping of the union tables (manually created)
			Collection<Set<String>> combinableTables = new LinkedList<>();
			
			BufferedReader r = new BufferedReader(new FileReader(union_tables_mapping));
			String line = null;
			while((line=r.readLine())!=null) {
				String[] content = line.split("\\#");
				String[] values = content[0].trim().split(",");
				combinableTables.add(new HashSet<>(Arrays.asList(values)));
				
				Map<Integer, Set<String>> columnMapping = new HashMap<>();
				for(String table : values) {
					provenanceMapping.put(table, columnMapping);
				}
			}
			r.close();
			
			// index tables by name
			HashMap<String, Table> tablesByName = new HashMap<>();
			for(Table t : allTables) {
				tablesByName.put(getTableId(t), t);
			}
			
			// add correspondences between union tables
			// format:
			// [representative table1];[column index1],[representative table2];[column index2], ...
			// each row is a final cluster in the gold standard
			
			r = new BufferedReader(new FileReader(inter_union_mapping));
	
			try {
				while((line = r.readLine())!=null) {
					String[] content = line.split("\\#");
					String[] values = content[0].trim().split(",");
					
					Set<String> prov = new HashSet<>();
					finalClusters.add(prov);
					
					Set<String> unionTables = new HashSet<>();
					
					for(String cor : values) {
						String[] parts = cor.split(";");
						
						String tbl1 = parts[0];
						Integer col1 = Integer.parseInt(parts[1]);
						
						// set prov for all combinable tables of tbl1 
						Set<String> combinableTablesOfTbl1 = null;
						for(Set<String> s : combinableTables) {
							if(s.contains(tbl1)) {
								combinableTablesOfTbl1 = s;
								break;
							}
						}
						
						if(combinableTablesOfTbl1==null) {
							throw new IndexOutOfBoundsException(String.format("[GoldStandard Error] Table %s was not assigned to any cluster!", tbl1));
						}
						
						for(String s : combinableTablesOfTbl1) {
							provenanceMapping.get(s).put(col1, prov);
						}
						
						// create the goldstandard for union tables
						for(Table t : allTables) {
							if(combinableTablesOfTbl1.contains(getTableId(t))) {
								
								int extraColumns = 0;
								for(TableColumn c : t.getColumns()) {
									if(SpecialColumns.isSpecialColumn(c) || ContextColumns.isContextColumn(c)) {
										extraColumns++;
									}
								}
								
								try {
									TableColumn c = t.getSchema().get(col1+extraColumns);
									unionTables.add(c.getIdentifier());
								} catch(IndexOutOfBoundsException ex) {
									throw new IndexOutOfBoundsException(String.format("[GoldStandard Error] Table %s (Cluster of %s) has no column index %d", getTableId(t), tbl1, col1));
								}
							}
						}
					}
	
					unionGold.getCorrespondenceClusters().put(unionTables, content[1]);
				}
			}
			finally {
				r.close();
			}
	
			for(Table t : allTables) {
	
				int extraColumns = 0;
				
				// iterate over all columns
				for(TableColumn c : t.getColumns()) {
					
					
					if(!SpecialColumns.isSpecialColumn(c) && !ContextColumns.isContextColumn(c)) {
						// either use the pre-merged set for provenance (if correspondences exist) or create a new set for this column
						
						Set<String> combinedProvenance = provenanceMapping.get(getTableId(t)).get(c.getColumnIndex()-extraColumns);
						
						if(combinedProvenance==null) {
							combinedProvenance = new HashSet<>();
							provenanceMapping.get(getTableId(t)).put(c.getColumnIndex()-extraColumns, combinedProvenance);
							finalClusters.add(combinedProvenance);
						}
		
						// the the provenance information
						combinedProvenance.addAll(c.getProvenance());
					
					} else {
						extraColumns++;
					}
					
				}
				
			}
			
			// assign names to the clusters
			for(Table t : allTables) {
				int extraColumns=0;
				for(TableColumn c : t.getColumns()) {
					if(!SpecialColumns.isSpecialColumn(c) && !ContextColumns.isContextColumn(c)) {
						Set<String> combinedProvenance = provenanceMapping.get(getTableId(t)).get(c.getColumnIndex()-extraColumns);
						if(!finalClusterNames.containsKey(combinedProvenance)) {
							finalClusterNames.put(combinedProvenance, String.format("[%d]%s{%s}", c.getColumnIndex(), c.getHeader(), t.getPath()));
						}
					} else {
						extraColumns++;
					}
				}
			}
			
			for(Set<String> s : finalClusters) {
				if(s!=null && finalClusterNames.get(s)!=null) {
					gold.getCorrespondenceClusters().put(s, finalClusterNames.get(s));
				}
			}
			
			gold.writeToTSV(new File(evaluationLocation, "goldstandard.tsv"));
			unionGold.writeToTSV(new File(evaluationLocation, "union_goldstandard.tsv"));
		}
	}
	
	protected String getTableId(Table t) {
		TableSchemaStatistics schema = new TableSchemaStatistics();
		return schema.generateNonContextSchemaString(t).replaceAll(",", "").replaceAll("\\#", "");
	}
	
	public void createFromMappedUnionColumns(Collection<Table> allTables, File evaluationLocation) throws IOException {
		// create a gold standard from a manual mapping of the union tables
		N2NGoldStandard gold = new N2NGoldStandard();
		// read the mapping of the union tables (manually created)
		Collection<Set<String>> combinableColumns = new LinkedList<>();
		Map<String, Set<String>> mergedProvenance = new HashMap<>();
		Map<Set<String>, String> mergedProvenanceNames = new HashMap<>();
		File union_tables_mapping = new File(evaluationLocation, "union_tables_full_mapping.txt");
		BufferedReader r = new BufferedReader(new FileReader(union_tables_mapping));
		String line = null;
		while((line=r.readLine())!=null) {
			String[] values = line.split(",");
			combinableColumns.add(new HashSet<>(Arrays.asList(values)));
			
			Set<String> prov = new HashSet<>();
			String name = null;
			for(String s : values) {
				mergedProvenance.put(s, prov);
				
				if(name == null) {
					name = s;
				}
			}
		
			mergedProvenanceNames.put(prov, name);
		}
		r.close();
		
    	for(Table t : allTables) {
    		for(TableColumn c : t.getColumns()) {
    			// get the set with the merged provenance
    			Set<String> mergedProv = mergedProvenance.get(String.format("[%d]%s{%s}", c.getColumnIndex(), c.getHeader(), t.getPath()));
    			// add the current column's provenance
    			mergedProv.addAll(c.getProvenance());
    			
    		}
    	}
    	
    	for(Set<String> prov : mergedProvenanceNames.keySet()) {
    		gold.getCorrespondenceClusters().put(prov, mergedProvenanceNames.get(prov));
    	}
    	gold.writeToTSV(new File(evaluationLocation, "union_tables_mapping_goldstandard.tsv"));
	}

	public void writeInterUnionMapping(File f, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences, final WebTables web) throws IOException {
		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
		
		for(MatchableTableColumn c : web.getSchema().get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(c, c, 1.0));
		}
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : correspondences.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clusters = clusterer.createResult();
		BufferedWriter w = new BufferedWriter(new FileWriter(f));
		
		int extra = 0;
		Table anyTable = Q.firstOrDefault(web.getTables().values());
		for(TableColumn c : anyTable.getColumns()) {
			if(ContextColumns.isContextColumn(c)) {
				extra++;
			}
		}
		
		final int extraColumns = extra;
		
		for(Collection<MatchableTableColumn> clu : clusters.keySet()) {
			clu = Q.where(clu, new Func<Boolean, MatchableTableColumn>() {

				@Override
				public Boolean invoke(MatchableTableColumn in) {
					return !ContextColumns.isContextColumn(in);
				}});
			
			if(clu.size()>0) {
				String columnIdentifiers = StringUtils.join(Q.project(clu, new Func<String, MatchableTableColumn>() {
	
					@Override
					public String invoke(MatchableTableColumn in) {
						return String.format("%s;%d", getTableId(web.getTables().get(in.getTableId())), in.getColumnIndex()-extraColumns);
					}}), ",");
				
				Distribution<String> headers = Distribution.fromCollection(clu, new MatchableTableColumn.ColumnHeaderProjection());
				
				w.write(String.format("%s\t#%s: %s\n", columnIdentifiers, headers.getMode(), headers.getElements()));
			}
		}
		
		w.close();
	}
}
