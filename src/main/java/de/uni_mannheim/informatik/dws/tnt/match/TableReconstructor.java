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

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * 
 * Merges tables based on the schema correspondences that are provided. First, the tables are transformed into a global schema (by merging all attributes). Then, the union of all transformed tables is created.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableReconstructor {

	private boolean addProvenance = true;
	private Map<Integer, Table> web = null;
	
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
	public TableReconstructor(Map<Integer, Table> web) {
		this.addProvenance = true;
		this.web = web;
	}
	
	protected Map<Collection<Integer>, Integer> getTableClusters(
			Processable<MatchableTableColumn> attributes, 
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		ConnectedComponentClusterer<Integer> clusterer = new ConnectedComponentClusterer<>();
		
		for(MatchableTableColumn c : attributes.get()) {
			clusterer.addEdge(new Triple<Integer, Integer, Double>(c.getTableId(), c.getTableId(), 1.0));
		}
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<Integer, Integer, Double>(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId(), cor.getSimilarityScore()));
		}
		
		return clusterer.createResult();
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> getCorrespondencesForTableCluster(
		Collection<Integer> cluster,
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> filtered = new ProcessableCollection<>();
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			if(cluster.contains(cor.getFirstRecord().getTableId())) {
				filtered.add(cor);
			}
		}
		
		return filtered;
	}
	
	protected Processable<MatchableTableColumn> getAttributesForTableCluster(
			Collection<Integer> cluster,
			Processable<MatchableTableColumn> attributes) {
		
		Processable<MatchableTableColumn> filtered = new ProcessableCollection<>();
		
		for(MatchableTableColumn col : attributes.get()) {
			if(cluster.contains(col.getTableId())) {
				filtered.add(col);
			}
		}
		
		return filtered;
	}
	
	protected Processable<MatchableTableRow> getRecordsForTableCluster(
			Collection<Integer> cluster,
			Processable<MatchableTableRow> records) {
		
		Processable<MatchableTableRow> filtered = new ProcessableCollection<>();
		
		for(MatchableTableRow row : records.get()) {
			if(cluster.contains(row.getTableId())) {
				filtered.add(row);
			}
		}
		
		return filtered;
	}
	
	protected Processable<MatchableTableDeterminant> getMappedKeysForTableCluster(
			Collection<Integer> cluster,
			Processable<MatchableTableDeterminant> keys,
			Processable<MatchableTableColumn> attributes) {
		
		Processable<MatchableTableDeterminant> result = new ProcessableCollection<>();
		
		Set<MatchableTableColumn> attributesForCluster = new HashSet<>(getAttributesForTableCluster(cluster, attributes).get());
		
		for(MatchableTableDeterminant k : keys.get()) {
			
			if(attributesForCluster.containsAll(k.getColumns())) {
				result.add(k);
			}
			
		}
		
		return result;
	}
	
	protected Map<Collection<MatchableTableColumn>, MatchableTableColumn> getAttributeClusters(
			Processable<MatchableTableColumn> attributes, 
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
		
		for(MatchableTableColumn c : attributes.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(c, c, 1.0));
		}
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		
		return clusterer.createResult();
	}
	
	protected Pair<Table, Map<MatchableTableColumn, TableColumn>> createReconstructedTable(
			int tableId, 
			Map<Collection<MatchableTableColumn>, 
			MatchableTableColumn> attributeClusters,
			Processable<MatchableTableDeterminant> mappedKeys) {
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
					TableColumn col = web.get(c0.getTableId()).getSchema().get(c0.getColumnIndex());
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
		for(MatchableTableDeterminant k : mappedKeys.get()) {
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
			int originalColumns = web.get(c.getTableId()).getSchema().get(c.getColumnIndex()).getProvenance().size();
			MapUtils.add(numOriginalColumnsPerHeader, c.getHeader(), originalColumns);
		}
		return MapUtils.max(numOriginalColumnsPerHeader);
	}
	
	protected Table populateTable(
			Table t, 
			Map<MatchableTableColumn, TableColumn> attributeMapping, 
			Processable<MatchableTableRow> records) {
		
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
				TableRow originalRow = web.get(row.getTableId()).get(row.getRowNumber());
				newRow.addProvenanceForRow(originalRow);
				newRow.getProvenance().add(row.getIdentifier());
			}
			
			t.addRow(newRow);
		}
		
		return t;
	}
	
	public Collection<Table> reconstruct(
			int firstTableId,
			Processable<MatchableTableRow> records,
			Processable<MatchableTableColumn> attributes, 
			Processable<MatchableTableDeterminant> candidateKeys,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		Map<Collection<Integer>, Integer> tableClusters = getTableClusters(attributes, schemaCorrespondences);
		
		Collection<Table> result = new ArrayList<>(tableClusters.size());
		
		int tableId = firstTableId;
		for(Collection<Integer> tableCluster : tableClusters.keySet()) {
		
			Processable<Correspondence<MatchableTableColumn, Matchable>> clusterCorrespondences = getCorrespondencesForTableCluster(tableCluster, schemaCorrespondences);
			Processable<MatchableTableColumn> clusterAttributes = getAttributesForTableCluster(tableCluster, attributes);
			
			Map<Collection<MatchableTableColumn>, MatchableTableColumn> attributeClusters = getAttributeClusters(clusterAttributes, clusterCorrespondences);
			
			Processable<MatchableTableDeterminant> mappedKeys = getMappedKeysForTableCluster(tableCluster, candidateKeys, clusterAttributes);
			
			Pair<Table, Map<MatchableTableColumn, TableColumn>> reconstruction = createReconstructedTable(tableId++, attributeClusters, mappedKeys);
			
			Processable<MatchableTableRow> clusterRecords = getRecordsForTableCluster(tableCluster, records);
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
