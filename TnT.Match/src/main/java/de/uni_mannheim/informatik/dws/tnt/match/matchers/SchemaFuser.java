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
package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.normalisation.ListHandler;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaFuser {

	/**
	 * 
	 * Merges t1 and t2 by adding all un-mapped columns of t1 to t2. The result indicates which column of t2 represents the columns from t1
	 * 
	 * @param t1
	 * @param t2
	 * @return returns the mapping of columns of t1 to columns of t2  
	 */
	public Map<TableColumn, TableColumn> fuseSchemas(Table t1, Table t2, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping) {

		Map<TableColumn, TableColumn> mappedColumns = new HashMap<>();
		
		// map column indices from t1 to t2
		Map<Integer, Integer> columnIndexMap = new HashMap<>();
		// map source table column
		columnIndexMap.put(0, 0);
		// map row number column
		columnIndexMap.put(1, 1);
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaMapping.get()) {
			columnIndexMap.put(cor.getFirstRecord().getColumnIndex(), cor.getSecondRecord().getColumnIndex());
		}
		

    	/***********************************************
    	 * Merge Schemas
    	 ***********************************************/
		
		// add all unmapped columns from t1 to t2
		int colIdx = t2.getColumns().size();
		for(TableColumn c : t1.getColumns()) {
			
			// if we don't have a mapping for the column
			if(!columnIndexMap.containsKey(c.getColumnIndex())) {
				
				// add the column to table t2
				TableColumn colNew = c.copy(t2, colIdx);
				t2.getSchema().addColumn(colNew);
				
				// add the index of the new column to the mapping
				columnIndexMap.put(c.getColumnIndex(), colIdx);
				
				colIdx++;
				
				mappedColumns.put(c, colNew);
			} 
			// if we have a mapping, add the column header
			else {

				// get the current header
				TableColumn t2Column = t2.getSchema().get(columnIndexMap.get(c.getColumnIndex()));
//				t2Column.addProvenanceForColumn(c);
				String header = t2Column.getHeader();
				
				HashSet<String> newHeaders;
				if(ListHandler.checkIfList(header)) {
					// if it is already a list, parse it
					String[] headers = ListHandler.splitList(header);
					newHeaders = new HashSet<>(Arrays.asList(headers));
				} else {
					// otherwise, create a new list
					newHeaders = new HashSet<>();
					newHeaders.add(header);
				}
				// and add the new header
				newHeaders.add(c.getHeader());
				
				// if the headers are not all equal
				if(newHeaders.size()>1) {
					// format the new header as list
					header = ListHandler.formatList(new ArrayList<>(newHeaders));
				}
				
				// and overwrite the existing header
//				t2Column.setHeader(header);
				
				mappedColumns.put(c, t2Column);
			}
		}
		
		return mappedColumns;
	}
	
	/***
	 * Merges the rows of t1 to t2 while mapping columns as stated in mappedColumns
	 * @param t1
	 * @param t2
	 * @param mappedColumns
	 * @return returns a mapping of rows of t1 to rows of t2
	 */
	public Map<TableRow, TableRow> mergeRows(Table t1, Table t2, Map<TableColumn, TableColumn> mappedColumns, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {

		Map<TableRow, TableRow> mappedRows = new HashMap<>();
		
		// create a map (row)->(row) for the instance correspondences
		HashMap<String, Integer> rowIndexMap = new HashMap<>();
		for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : instanceCorrespondences.get()) {
			rowIndexMap.put(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getRowNumber());
		}
		
		// copy rows from t1 to t2
		int rowIdx = t2.getRows().size();
		
		for(TableRow r : t1.getRows()) {

			boolean createNewRow = false;
			
			// if there is an instance correspondence, merge the rows
			if(rowIndexMap.containsKey(r.getIdentifier())) {
				
				TableRow existingRow = t2.getRows().get(rowIndexMap.get(r.getIdentifier()));
				
				// create a new value array
				Object[] values = new Object[t2.getColumns().size()];
				
				// copy the existing values
				for(TableColumn c : t2.getColumns()) {
					values[c.getColumnIndex()] = existingRow.get(c.getColumnIndex());
				}
				
				boolean wasEmpty = existingRow.getValueArray().length==0;
				
				// copy the new values
				for(TableColumn c : t1.getColumns()) {
					TableColumn t2Col = mappedColumns.get(c);
					Object oldValue = values[t2Col.getColumnIndex()];
					Object newValue = r.get(c.getColumnIndex());
					
					// if the column is the 'source table' column or 'row number' column
					if(SpecialColumns.isSpecialColumn(c)) {
						
						if(oldValue==null) {
							// if this is a new table, there is no 'source table' value
							
							values[t2Col.getColumnIndex()] = newValue;
						} else {
							// otherwise, merge the 'source table' values
							
							HashSet<String> sources = new HashSet<>();
							// merge the values of the source table column
							if(ListHandler.checkIfList((String)oldValue)) {
								sources.addAll(Arrays.asList(ListHandler.splitList((String)oldValue)));
							} else {
								sources.add((String)oldValue);
								sources.add((String)newValue);
							}
							
							values[t2Col.getColumnIndex()] = ListHandler.formatList(new ArrayList<>(sources));
						}
					} 
					// the column is a normal column
					else {
						values[t2Col.getColumnIndex()] = r.get(c.getColumnIndex());
						
						if(oldValue!=null && !oldValue.equals(newValue)) {
							// if there is a data conflict, the correspondence is not a correct 1:1 correspondence
							// in this case, just create a new row for the data
							createNewRow = true;
							
							if(wasEmpty) {
								//System.out.println("Two columns of one table mapped to the same output column!");
								
								// if multiple columns of one table are mapped to the same column in another table,
								// we cannot merge the values (if they are conflicting), so we split the one row into multiple rows
								
								// as the existing row is empty, we can set the values there
								// and still create a new row for the conflicting value
								existingRow.set(values);
								
								// the new row will be assigned the other value:
								// we will iterate the columns in the same order, and the value assigned to the existing row is encountered first
								// hence, it will be overwritten by the second value
								// TODO won't work if three columns are mapped to the same output column ...
							}
						}
					}
				} 
				
				// update the existing row if there was no conflict
				if(!createNewRow) {
					existingRow.set(values);
				} else {
					String sourceTable = ((String)existingRow.get(0));
					if(existingRow.getValueArray().length==0) {
						System.out.println("Unmerged row!");
					}
				}
			
				mappedRows.put(r, existingRow);
			} else {
				createNewRow = true;
			}
			
			// if there is no instance correspondence, add a new row
			if(createNewRow) {
				TableRow newRow = new TableRow(rowIdx++, t2);
				
				Object[] values = new Object[t2.getColumns().size()];
				
				for(TableColumn c : t1.getColumns()) {
					TableColumn t2Col = mappedColumns.get(c);
					values[t2Col.getColumnIndex()] = r.get(c.getColumnIndex());
				}
				
				newRow.set(values);
				newRow.addProvenanceForRow(r);
				
				t2.addRow(newRow);
				
				mappedRows.put(r, newRow);
			}
		}

		return mappedRows;
	}
}
