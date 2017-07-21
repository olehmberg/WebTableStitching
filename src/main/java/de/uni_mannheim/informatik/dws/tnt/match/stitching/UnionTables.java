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
package de.uni_mannheim.informatik.dws.tnt.match.stitching;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableContext;

/**
 * 
 * Creates union tables from a collection of tables.
 * All tables with the same schema are merged into the same union table
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class UnionTables {

	// maps a schema to a representative table for that schema
	HashMap<String, Table> schemaToTable = new HashMap<>();

	ContextAttributeExtractor context = new ContextAttributeExtractor();

	public Map<String, Integer> generateContextAttributes(Collection<Table> tables, boolean fromUriFragments, boolean fromUriQuery) throws URISyntaxException {
    	/***********************************************
    	 * Context Columns
    	 ***********************************************/
    	// first iterate over all tables and collect the possible context attributes then add all these attributes to all tables
    	// needed to make sure all tables have the same schema w.r.t. the added columns
		
		// maps the added context attributes to their relative column index
		Map<String, Integer> contextAttributes = new HashMap<>();
		
		contextAttributes.put(ContextColumns.PAGE_TITLE_COLUMN, 0);
		contextAttributes.put(ContextColumns.TALBE_HEADING_COLUMN, 1);
		
    	List<String> fragmentAttributes = new LinkedList<>();
    	List<String> queryAttributes = new LinkedList<>();
    	for(Table t : tables) {
    		for(String s : context.getUriFragmentParts(t)) {
    			if(!fragmentAttributes.contains(s)) {
    				fragmentAttributes.add(s);
    			}
    		}
    		for(String s : context.getUriQueryParts(t)) {
    			if(!queryAttributes.contains(s)) {
    				queryAttributes.add(s);
    			}
    		}
    	}

    	int colIdx = contextAttributes.size();
    	
    	if(fromUriFragments) {
	    	for(int i=0; i<fragmentAttributes.size(); i++) {
	    		contextAttributes.put(fragmentAttributes.get(i), colIdx++);
	    	}
    	}
    	if(fromUriQuery) {
	    	for(int i=0; i<queryAttributes.size(); i++) {
	    		contextAttributes.put(queryAttributes.get(i), colIdx++);
	    	}
    	}
    	
    	return contextAttributes;
	}
	
	public Collection<Table> create(List<Table> tables, Map<String, Integer> contextAttributes) {
	
    	/***********************************************
    	 * Create Union Tables (Schema)
    	 ***********************************************/
    	// the table schema statistics
    	TableSchemaStatistics stat = new TableSchemaStatistics();
    	
    	// sort the tables by file name to get reproducible table names for the output
    	Collections.sort(tables, new Comparator<Table>() {

			@Override
			public int compare(Table o1, Table o2) {
				return o1.getPath().compareTo(o2.getPath());
			}});
    	
    	// iterate over all tables and calculate schema statistics
    	for(Table t : tables) {
    		
    		// add the table
    		stat.addTable(t);
    		
    		// generate the schema (ordered set of all column headers)
    		String schema = getTableSchema(t);
    		
    		// add a representative table for that schema, if none exists yet
    		if(!schemaToTable.containsKey(schema)) {

    			try {
					createUnionTable(t, contextAttributes);
				} catch (Exception e) {
					e.printStackTrace();
				}
    	
    		}
    	}

    	// print the schema statistics to the console
    	stat.printSchemaList();
		
    	/***********************************************
    	 * Create Union Tables (Data)
    	 ***********************************************/
    	
    	Processable<Table> allTables = new ParallelProcessableCollection<>(tables);
    	
    	allTables.foreach(new DataIterator<Table>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void next(Table t) {
				// generate the schema (ordered set of all column headers)
	    		String schema = getTableSchema(t);
				
				// get the table that represents t's schema (and which will be the union table)
				Table union = schemaToTable.get(schema);
				
				if(union!=null) {
				
					synchronized (union) {
					
						// add provenance information to the columns
						for(TableColumn c : union.getColumns()) {
							int extraColumns = contextAttributes.size();
							
							if(!SpecialColumns.isSpecialColumn(c) && !ContextColumns.isContextColumn(c)) {
								TableColumn c2 = t.getSchema().get(c.getColumnIndex() - extraColumns);
								c.addProvenanceForColumn(c2);
							}
						}
						
						try {
							context.addUnionColumns(t, true, contextAttributes);
							
							union.append(t);
						} catch (URISyntaxException e) {
							e.printStackTrace();
						}
				
					}
					
				}
			}
			
			@Override
			public void initialise() {
			}
			
			@Override
			public void finalise() {
			}
		});
    	
    	return schemaToTable.values();
	}
	
	private String getTableSchema(Table t) {
		// generate the schema (ordered set of all column headers)
		String schema = String.format("%s", TableSchemaStatistics.generateSchemaString(t));
		return schema;
	}
	
	private Table createUnionTable(Table t, Map<String, Integer> contextAttributes) throws Exception {
		// add a copy of the table as new union table
		Table union = t.project(t.getColumns());
		union.setTableId(schemaToTable.size());
		union.setPath(Integer.toString(union.getTableId()));
		
		// add context information
		TableContext ctx = new TableContext();
		if(t.getContext()!=null) {
			ctx.setTableNum(t.getContext().getTableNum());
			URL url = new URL(t.getContext().getUrl());
			ctx.setUrl(String.format("%s://%s", url.getProtocol(), url.getHost()));
		}
		union.setContext(ctx);
		
		// remove all rows from the union table
		union.clear();
		
		// reset  the provenance information for the table
		for(TableColumn c : t.getColumns()) { 
			if(!SpecialColumns.isSpecialColumn(c)) {
				TableColumn uc = union.getSchema().get(c.getColumnIndex());
				List<String> prov = new LinkedList<String>();
				uc.setProvenance(prov);
				prov = new LinkedList<String>();
				c.setProvenance(prov);
			}
		}
		
		// add the context attributes
		context.addUnionColumns(union, false, contextAttributes);
		
		String schema = getTableSchema(t);
		
		schemaToTable.put(schema, union);
		
		return union;
	}
	
}
