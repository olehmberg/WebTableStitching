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
package de.uni_mannheim.informatik.dws.tnt.match.tasks;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;

import com.beust.jcommander.Parameter;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table.ConflictHandling;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.JsonTableWriter;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.parallel.ParallelDataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableNormalisation extends TnTTask {

	@Parameter(names = "-web")
	private String webLocation;
	/**
	 * @param webLocation the webLocation to set
	 */
	public void setWebLocation(String webLocation) {
		this.webLocation = webLocation;
	}

	@Parameter(names = "-results")
	private String resultLocation;
	/**
	 * @param resultLocation the resultLocation to set
	 */
	public void setResultLocation(String resultLocation) {
		this.resultLocation = resultLocation;
	}
	
	@Parameter(names = "-serialise")
	private boolean serialise;
	/**
	 * @param serialise the serialise to set
	 */
	public void setSerialise(boolean serialise) {
		this.serialise = serialise;
	}
	
	@Parameter(names = "-replaceNULLs")
	private boolean replaceNULLs = false;
	/**
	 * @param replaceNULLs the replaceNULLs to set
	 */
	public void setReplaceNULLs(boolean replaceNULLs) {
		this.replaceNULLs = replaceNULLs;
	}
	
	public static void main(String[] args) throws Exception {
		TableNormalisation td = new TableNormalisation();

		if (td.parseCommandLine(TableNormalisation.class, args)) {

			hello();

			td.initialise();

			td.setDataProcessingEngine(new ParallelDataProcessingEngine());
//			td.setDataProcessingEngine(new DataProcessingEngine());
			
			td.match();

		}
	}

	private WebTables web;
	private File resultLocationFile;

	public void initialise() throws IOException {
		printHeadline("Table De-Duplication");
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);

		// create output directory
		resultLocationFile = new File(resultLocation);
		resultLocationFile.mkdirs();
	}
	
	public void match() throws Exception {
    	/***********************************************
    	 * Schema Statistics
    	 ***********************************************/
    	// maps a table id to a schema
    	final HashMap<Integer, String> tableToSchema = new HashMap<>();
    	// maps a schema to a representative table for that schema
    	HashMap<String, Table> schemaToTable = new HashMap<>();
    	// the table schema statistics
    	final TableSchemaStatistics stat = new TableSchemaStatistics();
    	
    	ResultSet<Table> webTables = new ResultSet<>();
    	
    	// iterate over all tables and calculate schema statistics
    	for(Table t : web.getTables().values()) {
    		
    		// add the table
    		stat.addTable(t);
    		
    		// generate the schema (ordered set of all column headers)
    		String schema = stat.generateSchemaString(t);
    		
    		// add the lookup from table id to schema
    		tableToSchema.put(web.getTableIndices().get(t.getPath()), schema);
    		
    		// add a representative table for that schema, if none exists yet
    		if(!schemaToTable.containsKey(schema)) {
    			schemaToTable.put(schema, t);
    		} 

    		// add all the tables to a result set so we can process them with the matching framework
    		webTables.add(t);
    	}
    	
    	// print the schema statistics to the console
//    	stat.print();
//    	stat.printSchemaList();

    	for(Table t : Q.sort(web.getTables().values(), new Table.TablePathComparator())) {
    		System.out.println(String.format("%s\t%s", t.getPath(), stat.generateSchemaString(t)));
    	}
    	

    	/***********************************************
    	 * Functional Dependencies
    	 ***********************************************/
    	
    	// the location for the full tables in CSV format (for the calculation of the FDs)
    	final File CsvLocation = new File(new File(resultLocation), "normalised_csv");

    	// the location for the full tables in JSON format with FDs
    	final File JsonLocation = new File(new File(resultLocation), "normalised");

    	setOutputDirectory(JsonLocation);
    	
		CsvLocation.mkdirs();
		JsonLocation.mkdirs();
		
		final AtomicInteger tableId = new AtomicInteger(Q.max(web.getTables().keySet())+1);
		CSVTableWriter csvw = new CSVTableWriter();
		JsonTableWriter jsonw = new JsonTableWriter();
		
//		for(Table t : webTables.get()) {
		webTables = proc.transform(webTables, new RecordMapper<Table, Table>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public void mapRecord(Table record, DatasetIterator<Table> resultCollector) {
				Table t = record;
				StringBuilder logData = new StringBuilder();
				logData.append(t.getPath() + "\n");
				
				try {
					/***********************************************
	    	    	 * Normalisation to BCNF
	    	    	 ***********************************************/
					
					Func<String, TableColumn> headerP = new TableColumn.ColumnHeaderProjection();
					
					while(t!=null) {
						logData.append(String.format("Table #%d '%s' with schema '%s'\n", t.getTableId(), t.getPath(), stat.generateSchemaString(t)));
						
						List<Collection<TableColumn>> violations = new LinkedList<>();
						// iterate over all functional dependencies
						 for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
							 
							 Collection<TableColumn> closure = FunctionalDependencyUtils.closure(new HashSet<>(det), t.getSchema().getFunctionalDependencies());
							 
							 // check if the determinant is a candidate key
							 if(closure.size()<t.getColumns().size()) {
								 violations.add(det);
								 logData.append(String.format("\tBCNF violation: {%s}->{%s} [Closure: {%s}]\n", StringUtils.join(Q.project(det, headerP), ","), StringUtils.join(Q.project(t.getSchema().getFunctionalDependencies().get(det), headerP), ","), StringUtils.join(Q.project(closure, headerP), ",")));
							 }
						 }
						 if(violations.size()>0) {
							 // choose a violating functional dependency for decomposition
							 
							 Collections.sort(violations, new Comparator<Collection<TableColumn>>() {

								@Override
								public int compare(Collection<TableColumn> o1, Collection<TableColumn> o2) {
									return -Integer.compare(o1.size(), o2.size());
								}
							});
							 Collection<TableColumn> determinant = Q.firstOrDefault(violations);
							 Collection<TableColumn> dependant = t.getSchema().getFunctionalDependencies().get(determinant);
							 Collection<TableColumn> missing = Q.without(t.getColumns(), dependant);
							 
							 logData.append(String.format("\tChose {%s}->{%s} for decomposition with remainder {%s}\n", StringUtils.join(Q.project(determinant, headerP), ","), StringUtils.join(Q.project(dependant, headerP), ","), StringUtils.join(Q.project(missing, headerP), ",")));
							 
							 Table bcnf = t.project(Q.union(determinant, dependant));
							 logData.append(String.format("\t'%s' in BCNF with entity label '%s'\n", stat.generateSchemaString(bcnf), bcnf.getKeyIndex()==-1?"?":bcnf.getKey().getHeader()));
//							 bcnf.setTableId(tableId.getAndIncrement());
							 Table rest = t.project(Q.union(determinant, missing));
//							 rest.setTableId(tableId.getAndIncrement());
							 
							 resultCollector.next(bcnf);
//							 csvw.write(bcnf, new File(CsvLocation, Integer.toString(bcnf.getTableId())));
//							 jsonw.write(bcnf, new File(JsonLocation, Integer.toString(bcnf.getTableId())));
							 t = rest;
							 
						 } else {
							 // table is in BCNF
							 resultCollector.next(t);
							 logData.append(String.format("\t'%s' in BCNF with entity label '%s'\n", stat.generateSchemaString(t), t.getKeyIndex()==-1?"?":t.getKey().getHeader()));
//							 csvw.write(t, new File(CsvLocation, Integer.toString(t.getTableId())));
//							 jsonw.write(t, new File(JsonLocation, Integer.toString(t.getTableId())));
							 t = null;
						 }
					}
				} catch(Exception ex) {
					ex.printStackTrace();
				}
				
				System.out.println(logData.toString());
			}
			
		});
//		}
		
		// filter out tables that only contain context columns
		int tableIdx = 0;
		for(Table t : webTables.get()) {
			if(!Q.all(t.getColumns(), new ContextColumns.IsContextColumnPredicate())) {
				t.setTableId(tableIdx++);
				jsonw.write(t, new File(JsonLocation, Integer.toString(t.getTableId())));
				csvw.write(t, new File(CsvLocation, Integer.toString(t.getTableId())));
			}
		}
		
	}

}
