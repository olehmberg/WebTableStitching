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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Pair;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;

/**
 * 
 * Merges tables based on the schema correspondences that are provided. First, the tables are transformed into a global schema (by merging all attributes). Then, the union of all transformed tables is created.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableReconstructor {

	private boolean addProvenance = true;
	private WebTables web = null;
	
	/**
	 * Creates a TableReconstructor that does *not* add provenance
	 */
	public TableReconstructor() {
		addProvenance = false;
	}
	
	/**
	 * Creates a TableReconstructor that *does* add provenance
	 * @param web
	 */
	public TableReconstructor(WebTables web) {
		this.addProvenance = true;
		this.web = web;
	}
	
	protected Map<Collection<Integer>, Integer> getTableClusters(
			BasicCollection<MatchableTableColumn> attributes, 
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		
		ConnectedComponentClusterer<Integer> clusterer = new ConnectedComponentClusterer<>();
		
		for(MatchableTableColumn c : attributes.get()) {
			clusterer.addEdge(new Triple<Integer, Integer, Double>(c.getTableId(), c.getTableId(), 1.0));
		}
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<Integer, Integer, Double>(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId(), cor.getSimilarityScore()));
		}
		
		return clusterer.createResult();
	}
	
	protected ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> getCorrespondencesForTableCluster(
		Collection<Integer> cluster,
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> filtered = new ResultSet<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			if(cluster.contains(cor.getFirstRecord().getTableId())) {
				filtered.add(cor);
			}
		}
		
		return filtered;
	}
	
	protected ResultSet<MatchableTableColumn> getAttributesForTableCluster(
			Collection<Integer> cluster,
			BasicCollection<MatchableTableColumn> attributes) {
		
		ResultSet<MatchableTableColumn> filtered = new ResultSet<>();
		
		for(MatchableTableColumn col : attributes.get()) {
			if(cluster.contains(col.getTableId())) {
				filtered.add(col);
			}
		}
		
		return filtered;
	}
	
	protected ResultSet<MatchableTableRow> getRecordsForTableCluster(
			Collection<Integer> cluster,
			BasicCollection<MatchableTableRow> records) {
		
		ResultSet<MatchableTableRow> filtered = new ResultSet<>();
		
		for(MatchableTableRow row : records.get()) {
			if(cluster.contains(row.getTableId())) {
				filtered.add(row);
			}
		}
		
		return filtered;
	}
	
	protected ResultSet<MatchableTableKey> getMappedKeysForTableCluster(
			Collection<Integer> cluster,
			BasicCollection<MatchableTableKey> keys,
			BasicCollection<MatchableTableColumn> attributes) {
		
		ResultSet<MatchableTableKey> result = new ResultSet<>();
		
		Set<MatchableTableColumn> attributesForCluster = new HashSet<>(getAttributesForTableCluster(cluster, attributes).get());
		
		for(MatchableTableKey k : keys.get()) {
			
			if(attributesForCluster.containsAll(k.getColumns())) {
				result.add(k);
			}
			
		}
		
		return result;
	}
	
	protected Map<Collection<MatchableTableColumn>, MatchableTableColumn> getAttributeClusters(
			BasicCollection<MatchableTableColumn> attributes, 
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		
		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
		
		for(MatchableTableColumn c : attributes.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(c, c, 1.0));
		}
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		
		return clusterer.createResult();
	}
	
	protected Pair<Table, Map<MatchableTableColumn, TableColumn>> createReconstructedTable(
			int tableId, 
			Map<Collection<MatchableTableColumn>, 
			MatchableTableColumn> attributeClusters,
			ResultSet<MatchableTableKey> mappedKeys) {
		Table table = new Table();
		table.setPath(Integer.toString(tableId));
		table.setTableId(tableId);
		
		/*********************************
		 *  Column Clusters
		 *********************************/
		// get the attribute clusters
		Set<Collection<MatchableTableColumn>> schemaClusters = attributeClusters.keySet();
		
		int columnIndex = 0;
		
		Map<MatchableTableColumn, TableColumn> columnMapping = new HashMap<>();
		
		List<Collection<MatchableTableColumn>> sorted = Q.sort(schemaClusters, new Comparator<Collection<MatchableTableColumn>>() {

			@Override
			public int compare(Collection<MatchableTableColumn> o1, Collection<MatchableTableColumn> o2) {
				int idx1 = Q.min(o1, new MatchableTableColumn.ColumnIndexProjection()).getColumnIndex();
				int idx2 = Q.min(o2, new MatchableTableColumn.ColumnIndexProjection()).getColumnIndex();
				
				return Integer.compare(idx1, idx2);
			}
		});
		
		// map all columns in the cluster to a new column in the merged table
		for(Collection<MatchableTableColumn> schemaCluster : sorted) {
			
			TableColumn c = new TableColumn(columnIndex++, table);
			
			// use the most frequent header as the header for the merged column
			Distribution<String> headers = Distribution.fromCollection(schemaCluster, new MatchableTableColumn.ColumnHeaderProjection());
//			String newHeader = headers.getMode();
			String newHeader = getMostFrequentHeaderForCluster(schemaCluster);
			c.setHeader(newHeader);
			c.setSynonyms(headers.getElements());
			
			// use the most frequent data type
			Distribution<DataType> types = Distribution.fromCollection(schemaCluster, new Func<DataType, MatchableTableColumn>() {

				@Override
				public DataType invoke(MatchableTableColumn in) {
					return in.getType();
				}});
			c.setDataType(types.getMode());
			
			if(addProvenance) {
				// add provenance for column
				for(MatchableTableColumn c0 : schemaCluster) {
					TableColumn col = web.getTables().get(c0.getTableId()).getSchema().get(c0.getColumnIndex());
					c.addProvenanceForColumn(col);
				}
			}
			
			table.addColumn(c);
			
			for(MatchableTableColumn col : schemaCluster) {
				columnMapping.put(col, c);
			}
		}

		// add all candidate keys that are completely mapped
		Set<Set<TableColumn>> newKeys = new HashSet<>();
		for(MatchableTableKey k : mappedKeys.get()) {
			Set<TableColumn> keyInNewTable = new HashSet<>();
			
			for(MatchableTableColumn col : k.getColumns()) {
				TableColumn c = columnMapping.get(col);
				
				if(c==null) {
					break;
				} else {
					keyInNewTable.add(c);
				}
			}
			
			if(keyInNewTable.size()==k.getColumns().size()) {
				newKeys.add(keyInNewTable);
			}
			
		}
		for(Set<TableColumn> key : newKeys) {
			table.getSchema().getCandidateKeys().add(key);
		}
		
		return new Pair<Table, Map<MatchableTableColumn,TableColumn>>(table, columnMapping);
	}
	
	protected String getMostFrequentHeaderForCluster(Collection<MatchableTableColumn> schemaCluster) {
		Map<String, Integer> numOriginalColumnsPerHeader = new HashMap<>();
		for(MatchableTableColumn c : schemaCluster) {
			int originalColumns = web.getTables().get(c.getTableId()).getSchema().get(c.getColumnIndex()).getProvenance().size();
			MapUtils.add(numOriginalColumnsPerHeader, c.getHeader(), originalColumns);
		}
		return MapUtils.max(numOriginalColumnsPerHeader);
	}
	
	protected Table populateTable(
			Table t, 
			Map<MatchableTableColumn, TableColumn> attributeMapping, 
			BasicCollection<MatchableTableRow> records) {
		
		int rowIdx = 0;
		
		for(MatchableTableRow row : records.get()) {
		
			TableRow newRow = new TableRow(rowIdx++, t);
			
			Object[] values = new Object[t.getColumns().size()];
			
			for(MatchableTableColumn c : row.getSchema()) {
				TableColumn t2Col = attributeMapping.get(c);
				
				if(t2Col!=null) {
					Object value = row.get(c.getColumnIndex());
					if(value==null) {
						// replace empty cells with an empty string (will not make a difference in the written file)
						value = "";
						
						// all attributes which did not exist in the original tables will then have the value null, all others (even if empty) will have at least an empty string
						// this allows us to deduplicate afterwards and replace the nulls with actual values
						// the intuition is that values which have not been observed cannot cause a conflict and can hence be replaced by any observed value
					}
					values[t2Col.getColumnIndex()] = value;
				}
			}
			
			newRow.set(values);
			
			if(addProvenance) {
				// add provenance
				TableRow originalRow = web.getTables().get(row.getTableId()).get(row.getRowNumber());
				newRow.addProvenanceForRow(originalRow);
			}
			
			t.addRow(newRow);
		}
		
		return t;
	}
	
	public Collection<Table> reconstruct(
			int firstTableId,
			BasicCollection<MatchableTableRow> records,
			BasicCollection<MatchableTableColumn> attributes, 
			BasicCollection<MatchableTableKey> candidateKeys,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		
		Map<Collection<Integer>, Integer> tableClusters = getTableClusters(attributes, schemaCorrespondences);
		
		Collection<Table> result = new ArrayList<>(tableClusters.size());
		
		int tableId = firstTableId;
		for(Collection<Integer> tableCluster : tableClusters.keySet()) {
		
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> clusterCorrespondences = getCorrespondencesForTableCluster(tableCluster, schemaCorrespondences);
			ResultSet<MatchableTableColumn> clusterAttributes = getAttributesForTableCluster(tableCluster, attributes);
			
			Map<Collection<MatchableTableColumn>, MatchableTableColumn> attributeClusters = getAttributeClusters(clusterAttributes, clusterCorrespondences);
			
			ResultSet<MatchableTableKey> mappedKeys = getMappedKeysForTableCluster(tableCluster, candidateKeys, clusterAttributes);
			
			Pair<Table, Map<MatchableTableColumn, TableColumn>> reconstruction = createReconstructedTable(tableId++, attributeClusters, mappedKeys);
			
			ResultSet<MatchableTableRow> clusterRecords = getRecordsForTableCluster(tableCluster, records);
			Table t = populateTable(reconstruction.getFirst(), reconstruction.getSecond(), clusterRecords);
		
			result.add(t);
		}
		
		return result;
	}
	
	public Table removeSparseColumns(Table t, double minDensity) throws Exception {
		
		Set<TableColumn> sparseColumns = new HashSet<>();
		
		for(TableColumn c : t.getColumns()) {
			
			int values = 0;
			
			for(TableRow r : t.getRows()) {
				
				if(r.get(c.getColumnIndex())!=null) {
					values++;
				}
				
			}
			
			double density = values / (double)t.getRows().size();
			
			if(density<minDensity) {
				sparseColumns.add(c);
			}
			
		}
		
		Collection<TableColumn> newColumns = Q.without(t.getColumns(), sparseColumns);
		
		return t.project(newColumns);
	}
}
