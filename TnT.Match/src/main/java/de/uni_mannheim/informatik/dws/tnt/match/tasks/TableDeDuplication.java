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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;

import com.beust.jcommander.Parameter;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
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

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableDeDuplication extends TnTTask {

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
		TableDeDuplication td = new TableDeDuplication();

		if (td.parseCommandLine(TableDeDuplication.class, args)) {

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
		
		long start = System.currentTimeMillis();
		
    	/***********************************************
    	 * Schema Statistics
    	 ***********************************************/
    	// maps a table id to a schema
    	final HashMap<Integer, String> tableToSchema = new HashMap<>();
    	// maps a schema to a representative table for that schema
    	HashMap<String, Table> schemaToTable = new HashMap<>();
    	// the table schema statistics
    	TableSchemaStatistics stat = new TableSchemaStatistics();
    	// remember the number of rows before deduplication for statistics
    	Map<Integer, Integer> rowsBeforeDeduplication = new HashMap<>();
    	
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
    		
    		// set the number of rows before deduplication
    		rowsBeforeDeduplication.put(t.getTableId(), t.getRows().size());
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
    	final File CsvLocation = new File(new File(resultLocation), new File(webLocation).getName() + "_dedup_csv");

    	// the location for the full tables in JSON format with FDs
    	final File JsonFDLocation = new File(new File(resultLocation), new File(webLocation).getName() +  "_dedup_json");

    	setOutputDirectory(JsonFDLocation);
    	
		CsvLocation.mkdirs();
		JsonFDLocation.mkdirs();
    	
		PrintStream tmp = new PrintStream(new File("HyFD_UCC.out"));
		final PrintStream out = System.out;
		System.setOut(tmp);
		
		final AtomicInteger dedupCount = new AtomicInteger(0);
		
    	proc.iterateDataset(webTables, new DatasetIterator<Table>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void initialise() {
				
			}

			@Override
			public void next(final Table t) {
				StringBuilder logData = new StringBuilder();
				logData.append(t.getPath() + "\n");
				
				try {
					/***********************************************
	    	    	 * De-Duplication
	    	    	 ***********************************************/
					List<TableColumn> colsNoContext = new ArrayList<>(t.getColumns());
					
					Iterator<TableColumn> colIt = colsNoContext.iterator();
					while(colIt.hasNext()) {
						
						if(SpecialColumns.isSpecialColumn(colIt.next())) {//TODO can be removed as SpecialColumns are no longer inserted into the tables
							colIt.remove();
						}

					}
	    			
	    			logData.append(String.format("\tKey for De-Duplication: %s\n", StringUtils.join(Q.project(colsNoContext, new TableColumn.ColumnHeaderProjection()), ",")));
	    			int rowsTotal = t.getRows().size();
	    			
	    			// de-duplicate t on all columns except the context surrogate key (FDs can be calculated with duplicate rows, but UCCs not)
	    			if(replaceNULLs) {
	    				for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {
	    					t.deduplicate(key, ConflictHandling.ReplaceNULLs);
	    				}
	    			} else {
	    				t.deduplicate(colsNoContext);
	    			}

	    			int duplicateRows = rowsTotal - t.getRows().size();
	    			logData.append(String.format("\tDe-Duplication removed %d/%d rows.\n", duplicateRows, rowsTotal));
	    			
	    			dedupCount.addAndGet(duplicateRows);
	    			
					// write tables as csv to run FD discovery
	    			CSVTableWriter tw = new CSVTableWriter();	    			
	    			File f = tw.write(t, new File(CsvLocation, t.getPath()), ',', '"', '\\');
		    		
	    	    	/***********************************************
	    	    	 * Calculate Functional Dependencies
	    	    	 ***********************************************/
					Map<Collection<TableColumn>, Collection<TableColumn>> functionalDependencies = FunctionalDependencyUtils.calculateFunctionalDependencies(t, f);
					logFunctionalDependencies(logData, functionalDependencies, "Functional Dependencies");
					t.getSchema().setFunctionalDependencies(functionalDependencies);

		    		
		        	/***********************************************
		        	 * Key Detection
		        	 ***********************************************/
		    		
		    		// list all candidate keys that include the context surrogate key
		    		Collection<Set<TableColumn>> candKeysWithContext = FunctionalDependencyUtils.calculateUniqueColumnCombinations(t, f); 
		    		t.getSchema().setCandidateKeys(candKeysWithContext);
					logData.append("\tCandidate Keys with Context (HyUCC):\n");
		    		for(Collection<TableColumn> cand : candKeysWithContext) {
		    			logData.append(String.format("\t\t%s\n", StringUtils.join(Q.project(cand, new TableColumn.ColumnHeaderProjection()), ",")));
		    		}
		    		
		        	// write the de-duplicated tables in JSON format with FDs
		    		JsonTableWriter jtw = new JsonTableWriter();
		    		jtw.write(t, new File(JsonFDLocation, t.getPath()));
				} catch(Exception e) {
					e.printStackTrace();
				}
				
				out.println(logData.toString());
//				log.debug(logData.toString());
			}

			@Override
			public void finalise() {

			}
		});
    	
    	System.setOut(out);
    	
    	long duration = System.currentTimeMillis() - start;
    	
    	/***********************************************
    	 * Key & Dependency Statistics
    	 ***********************************************/
    	Distribution<String> keyStatistics = new Distribution<>();
    	Distribution<String> determinantStatistics = new Distribution<>();
    	Map<Table, Set<TableColumn>> contextDependentColumns = new HashMap<>();
    	Map<Table, Integer> contextColumns = new HashMap<>();
    	
    	for(Table t : web.getTables().values()) {
    		
    		HashSet<String> columns = new HashSet<>();
    		
    		for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {
//    			List<String> sorted = Q.sort(Q.project(key, new TableColumn.ColumnHeaderProjection()));
//    			String combined = String.format("{%s}", StringUtils.join(sorted, ","));
    			
    			for(TableColumn c : key) {
    				
    				String header = c.getHeader();
    				
    				if(header.equals("null")) {
    					header = String.format("{%s}[%d]%s", t.getPath(), c.getColumnIndex(), c.getHeader());
    				}
    				
    				columns.add(header);
    				
    			}
    		}
    		// only add a column once per table, even if it's found in more keys
    		for(String header : columns) {
    			keyStatistics.add(header);
    		}
    		
    		
    		columns.clear();
    		for(Collection<TableColumn> determinant : t.getSchema().getFunctionalDependencies().keySet()) {
    			
    			boolean contextInDeterminant = false;
    			
    			for(TableColumn c : determinant) {
    				
    				String header = c.getHeader();
    				
    				if(header.equals("null")) {
    					header = String.format("{%s}[%d]%s", t.getPath(), c.getColumnIndex(), c.getHeader());
    				}
    				
    				columns.add(header);
    				
    				if(ContextColumns.isContextColumn(c)) {
    					contextInDeterminant=true;
    				}
    			}
    			
    			if(contextInDeterminant) {
    				Set<TableColumn> cols = contextDependentColumns.get(t);
    				if(cols==null) {
    					cols = new HashSet<>();
    					contextDependentColumns.put(t, cols);
    				}
    				for(TableColumn c : t.getSchema().getFunctionalDependencies().get(determinant)) {
    					if(!ContextColumns.isContextColumn(c)) {
    						cols.add(c);
    					}
    				}
    			}
    			
    		}
    		
    		// only add a column once per table, even if it's found in more FD's
    		for(String header : columns) {
    			determinantStatistics.add(header);
    		}
    		
    		int extraColumns = 0;
    		for(TableColumn c : t.getColumns()) {
    			if(ContextColumns.isContextColumn(c)) {
    				extraColumns++;
    			}
    		}
    		contextColumns.put(t, extraColumns);
    	}
    	
    	System.out.println("Candidate Key Distribution");
    	System.out.println(keyStatistics.format());
    	
    	System.out.println("Column-Determinant Distribution");
    	System.out.println(determinantStatistics.format());
    	
    	System.out.println(String.format("Total number of duplicates: %d", dedupCount.get()));
    	
		CSVWriter resultStatisticsWriter = new CSVWriter(new FileWriter(new File(resultLocationFile.getParent(), new File(webLocation).getName() + "_deduplication.csv"), true));
		CSVWriter resultKeyWriter = new CSVWriter(new FileWriter(new File(resultLocationFile.getParent(), new File(webLocation).getName() + "_deduplication_candidate_keys.csv"), true));
		CSVWriter resultFDWriter = new CSVWriter(new FileWriter(new File(resultLocationFile.getParent(), new File(webLocation).getName() + "_deduplication_dependencies.csv"), true));
		for(Table t : Q.sort(web.getTables().values(), new Table.TableIdComparator())) {
			// write the statistics
			resultStatisticsWriter.writeNext(new String[] {
					resultLocationFile.getName(),
					t.getPath(),
					Integer.toString(rowsBeforeDeduplication.get(t.getTableId())),
					Integer.toString(t.getRows().size()),
					Integer.toString(t.getSchema().getFunctionalDependencies().size()),
					Integer.toString(t.getSchema().getCandidateKeys().size()),
					Integer.toString(t.getColumns().size()),
					Integer.toString(contextColumns.get(t)),
					Integer.toString(contextDependentColumns.containsKey(t) ? contextDependentColumns.get(t).size() : 0),
					Long.toString(duration)
			});
			
			for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {
				resultKeyWriter.writeNext(new String[] {
						resultLocationFile.getName(),
						t.getPath(),
						StringUtils.join(Q.project(key, new TableColumn.ColumnIndexAndHeaderProjection("\\+")), "+")
				});
			}
			for(Collection<TableColumn> determinant : t.getSchema().getFunctionalDependencies().keySet()) {
				Collection<TableColumn> dependant = t.getSchema().getFunctionalDependencies().get(determinant);
				
				resultFDWriter.writeNext(new String[] {
						resultLocationFile.getName(),
						t.getPath(),
						StringUtils.join(Q.project(determinant, new TableColumn.ColumnIndexAndHeaderProjection("\\+")), "+"),
						StringUtils.join(Q.project(dependant, new TableColumn.ColumnIndexAndHeaderProjection("\\+")), "+")
				});
			}
		}
		resultStatisticsWriter.close();
		resultKeyWriter.close();
		resultFDWriter.close();
	}

//	public void deduplicate(Table t, Collection<TableColumn> key) {
//    	/***********************************************
//    	 * De-Duplication
//    	 ***********************************************/
//
//		// use the provided key to perform duplicate detection
//		// keep a map of (key values)->(first row with these values) for the chosen key
//		HashMap<List<Object>, TableRow> seenKeyValues = new HashMap<>();
//		
//		// iterate the table row by row
//		Iterator<TableRow> rowIt = t.getRows().iterator();
//		while(rowIt.hasNext()) {
//			TableRow r = rowIt.next();
//			
//			// get the values of the super key for the current row
//			ArrayList<Object> keyValues = new ArrayList<>(key.size()); 
//			for(TableColumn c : key) {
//				keyValues.add(r.get(c.getColumnIndex()));
//			}
//			
//			// check if the super key values have been seen before
//			if(seenKeyValues.containsKey(keyValues)) {
//				// if so, remove the row as it must be duplicate
//				rowIt.remove();
//				// and add the table name of the duplicate row to the existing row
//				TableRow existing = seenKeyValues.get(keyValues);
//				String sourceTables = existing.get(0).toString();
//				HashSet<String> newSourceTables;
//				if(ListHandler.checkIfList(sourceTables)){
//					newSourceTables = new HashSet<String>(Arrays.asList(ListHandler.splitList(sourceTables)));
//					newSourceTables.add(t.getPath());
//				} else {
//					newSourceTables = new HashSet<String>();
//					newSourceTables.add(sourceTables);
//					newSourceTables.add(t.getPath());
//				}
//				existing.set(0, ListHandler.formatList(new ArrayList<>(newSourceTables)));
//				
//				// add provenance information
//				existing.addProvenanceForRow(r);
//			} else {
//				// if not, add the current super key values to the list of seen values
//				seenKeyValues.put(keyValues, r);
//				
//				// add the row itself as provenance information (so we have all source information if later rows are merged with this one)
//				r.addProvenanceForRow(r);
//			}
//		}
//		
//		t.reorganiseRowNumbers();
//	}

	protected void logFunctionalDependencies(StringBuilder logData, Map<Collection<TableColumn>, Collection<TableColumn>> functionalDependencies, String title) {
		logData.append(String.format("\t%s\n",title));
		for(Map.Entry<Collection<TableColumn>, Collection<TableColumn>> fd : functionalDependencies.entrySet()) {
			logData.append(String.format("\t\t{%s}->{%s}\n", 
					StringUtils.join(Q.project(fd.getKey(), new TableColumn.ColumnHeaderProjection()), ","), 
					StringUtils.join(Q.project(fd.getValue(), new TableColumn.ColumnHeaderProjection()), ",")));
		}
	}
}
