package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

//import objectexplorer.MemoryMeasurer;
//import objectexplorer.ObjectGraphMeasurer;
//import objectexplorer.ObjectGraphMeasurer.Footprint;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.t2k.webtables.features.HorizontallyStackedFeature;
import de.uni_mannheim.informatik.dws.t2k.webtables.parsers.CsvTableParser;
import de.uni_mannheim.informatik.dws.t2k.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.JsonTableWriter;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.FusableDataSet;
import de.uni_mannheim.informatik.wdi.utils.ProgressReporter;

public class WebTables {

    // data that will be matched: records and schema
	private FusableDataSet<MatchableTableRow, MatchableTableColumn> records = new FusableDataSet<>();
	private DefaultDataSet<MatchableTableColumn, MatchableTableColumn> schema = new DefaultDataSet<>();
	private DefaultDataSet<MatchableTableKey, MatchableTableColumn> candidateKeys = new DefaultDataSet<>();
	private DefaultDataSet<MatchableTable, MatchableTableColumn> tableRecords = new DefaultDataSet<>();
	
	// matched web tables and their key columns
	private HashMap<Integer, MatchableTableColumn> keys = new HashMap<>();
	
	// translation for web table identifiers
	private HashMap<String, String> columnHeaders = new HashMap<>();
	
	// translation from table name to table id
	private HashMap<String, Integer> tableIndices = new HashMap<>();
	
	// lookup for tables by id
	private HashMap<Integer, Table> tables = null;
	
	private boolean measure = false;
	
	public void setMeasureMemoryUsage(boolean measure) {
		this.measure = measure;
	}
	
	public void setKeepTablesInMemory(boolean keep) {
		if(keep) {
			tables = new HashMap<>();
		} else {
			tables = null;
		}
	}
	
	private boolean convertValues = true;
	/**
	 * @param convertValues the convertValues to set
	 */
	public void setConvertValues(boolean convertValues) {
		this.convertValues = convertValues;
	}
	
	private boolean inferSchema = true;
	/**
	 * @param inferSchema the inferSchema to set
	 */
	public void setInferSchema(boolean inferSchema) {
		this.inferSchema = inferSchema;
	}
	
	public static WebTables loadWebTables(File location, boolean keepTablesInMemory, boolean inferSchema, boolean convertValues, boolean serialise) throws FileNotFoundException {
    	// look for serialised version
		File ser = new File(location.getParentFile(), location.getName() + ".bin");
		
		if(ser.exists() && serialise) {
			WebTables web = WebTables.deserialise(ser);
			web.printLoadStats();
			return web;
		} else {
			WebTables web = new WebTables();
			web.setKeepTablesInMemory(keepTablesInMemory);
			web.setInferSchema(inferSchema);
			web.setConvertValues(convertValues);
	    	web.load(location);
	    	
	    	// Serialise only if we loaded more than one table (otherwise we would generate .bin files in folders that contain many web tables which would lead to problem when loading the whole folder)
	    	if(web.getRecords().getSize()>1 && serialise) {
	    		web.serialise(ser);
	    	}
	    	
	    	return web;
		}
	}
	
    public void load(File location) {
    	CsvTableParser csvParser = new CsvTableParser();
    	JsonTableParser jsonParser = new JsonTableParser();
    	
    	//TODO add setting for value conversion to csv parser
    	jsonParser.setConvertValues(convertValues);
    	jsonParser.setInferSchema(inferSchema);
    	
    	List<File> webFiles = null;
    	
    	if(location.isDirectory()) {
    		webFiles = Arrays.asList(location.listFiles());
    	} else {
    		webFiles = Arrays.asList(new File[] { location});
    	}
    	
    	ProgressReporter progress = new ProgressReporter(webFiles.size(), "Loading Web Tables");
    	
    	int tblIdx=0;
    	
    	Queue<File> toLoad = new LinkedList<>(webFiles);
//    	for(File f : webFiles) {
    	while(toLoad.size()>0) {
    		File f = toLoad.poll();
    		
    		if(f.isDirectory()) {
    			toLoad.addAll(Arrays.asList(f.listFiles()));
    			progress = new ProgressReporter(toLoad.size(), "Loading Web Tables", progress.getProcessedElements());
    		} else {
    		
//			System.out.println("Loading Web Table " + f.getName());
				try {
					Table web = null;
					
					if(f.getName().endsWith("csv")) {
						web = csvParser.parseTable(f);
					} else if(f.getName().endsWith("json")) {
						web = jsonParser.parseTable(f);
					} else {
						System.out.println(String.format("Unknown table format: %s", f.getName()));
					}
					
					if(web==null) {
						continue;
					}
					
					if(tables!=null) {
						tables.put(tblIdx, web);
						web.setTableId(tblIdx);
					}
					
					if(webFiles.size()==1) {
						for(TableColumn tc : web.getSchema().getRecords()) {
							System.out.println(String.format("{%s} [%d] %s (%s)", web.getPath(), tc.getColumnIndex(), tc.getHeader(), tc.getDataType()));
						}
					}
		    		
					tableIndices.put(web.getPath(), tblIdx);
					
			    	// list schema
					LinkedList<MatchableTableColumn> schemaColumns = new LinkedList<>();
			    	for(TableColumn c : web.getSchema().getRecords()) {
			    		MatchableTableColumn mc = new MatchableTableColumn(tblIdx, c);
		    			schema.addRecord(mc);
		    			schemaColumns.add(mc);
		    			columnHeaders.put(mc.getIdentifier(), c.getHeader());
		    			if(web.hasKey() && web.getKeyIndex()==c.getColumnIndex()) {
		    				keys.put(mc.getTableId(), mc);
		    			}
		    		}
			    	
			    	// list candidate keys
			    	for(Set<TableColumn> candKey : web.getSchema().getCandidateKeys()) {
			    		
			    		Set<MatchableTableColumn> columns = new HashSet<>(); 
			    		for(TableColumn keyCol : candKey) {
			    			for(MatchableTableColumn mc : schemaColumns) {
			    				if(mc.getColumnIndex()==keyCol.getColumnIndex()) {
			    					columns.add(mc);
			    				}
			    			}
			    		}
			    		
			    		MatchableTableKey k = new MatchableTableKey(tblIdx, columns);
			    		
			    		candidateKeys.add(k);
			    	}
			    	
			    	// create the matchable table record
			    	MatchableTable mt = new MatchableTable(web, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
			    	tableRecords.add(mt);
					
		    		// list records
			    	for(TableRow r : web.getRows()) {
			    		MatchableTableRow row = new MatchableTableRow(r, tblIdx, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
			    		records.addRecord(row);
			    	}
			    	
	
			    	tblIdx++;
				} catch(Exception e) {
					System.err.println(String.format("Could not load table %s", f.getAbsolutePath()));
					e.printStackTrace();
				}
				
				progress.incrementProgress();
				progress.report();
    		}
    	}
    	
    	printLoadStats();
    }
    
    public void removeHorizontallyStackedTables() throws Exception {
    	HorizontallyStackedFeature f = new HorizontallyStackedFeature();
    	TableSchemaStatistics stat = new TableSchemaStatistics();
    	
    	for(Integer tableId : new ArrayList<>(tables.keySet())) {
    		Table t = tables.get(tableId);
    		
    		Collection<TableColumn> noContextColumns = Q.where(t.getColumns(), new ContextColumns.IsNoContextColumnPredicate());
    		
    		Table tNoContext = t.project(noContextColumns);
    		
    		double horizontallyStacked = f.calculate(tNoContext);
    		
    		if(horizontallyStacked>0.0) {
    			
    			System.out.println(String.format("Removing table '%s' with schema '%s' (horizontally stacked)", t.getPath(), stat.generateNonContextSchemaString(t)));
    			
    			tables.remove(tableId);
    			
    			for(TableColumn c : t.getColumns()) {
    				columnHeaders.remove(c.getIdentifier());
    				schema.removeRecord(c.getIdentifier());
    				
    			}
    			
    			Iterator<MatchableTableKey> ckIt = candidateKeys.get().iterator();
    			while(ckIt.hasNext()) {
    				if(ckIt.next().getTableId()==tableId) {
    					ckIt.remove();
    				}
    			}
    			
    			for(TableRow r : t.getRows()) {
    				records.removeRecord(r.getIdentifier());
    			}
    			
    			keys.remove(tableId);
    			tableIndices.remove(t.getPath());
    			tableRecords.removeRecord(Integer.toString(t.getTableId()));
    		}
    	}
    }
    
    public Table verifyColumnHeaders(Table t) {
    	// check if the column headers are all null, if so, skip until a non-null row is found
    	
    	for(TableColumn c : t.getColumns()) {
    		if(c.getHeader()!=null && !c.getHeader().isEmpty() && !"null".equals(c.getHeader())) {
    			return t;
    		}
    	}
    	
    	// all headers are null
    	TableRow headerRow = null;
    	for(TableRow r : t.getRows()) {
    		for(TableColumn c : t.getColumns()) {
    			
    			Object value = r.get(c.getColumnIndex());
    			
    			if(value!=null && !"null".equals(value)) {
    				headerRow = r;
    			}
    		}
    	}
    	
    	if(headerRow!=null) {
    		Table t2 = t.copySchema();
    		
    		for(TableColumn c : t.getColumns()) {
    			Object value = headerRow.get(c.getColumnIndex());
    			String header = null;
    			if(value==null) {
    				header = "null";
    			} else {
    				header = value.toString();
    			}
    			t2.getSchema().get(c.getColumnIndex()).setHeader(header);
    		}
    		
    		int rowNumber = 0;
    		for(TableRow r : t.getRows()) {
    			if(r.getRowNumber()>headerRow.getRowNumber()) {
	    			TableRow r2 = new TableRow(rowNumber++, t2);
	    			t2.addRow(r2);
    			}
    		}
    		
    		return t2;
    	} else {
    		return t;
    	}
    }
    
    void printLoadStats() {
    	System.out.println(String.format("%,d Web Tables Instances", records.getSize()));
    	System.out.println(String.format("%,d Web Tables Schema Elements", schema.getSize()));
    	
    	if(measure) {
	    	System.out.println("Measuring Memory Usage");
	    	measure(records, "Web Tables Dataset");
	    	measure(schema, "Web Tables Schema");
	    	measure(columnHeaders, "Web Tables Column Headers");
	    	measure(keys, "Web Table Keys");
    	}
    }
    
    void measure(Object obj, String name) {
//        long memory = MemoryMeasurer.measureBytes(obj);
//
//        System.out.println(String.format("%s Memory Size: %,d", name, memory));
//        
//        Footprint footprint = ObjectGraphMeasurer.measure(obj);
//        System.out.println(String.format("%s Graph Footprint: \n\tObjects: %,d\n\tReferences %,d", name, footprint.getObjects(), footprint.getReferences()));
    }

	public FusableDataSet<MatchableTableRow, MatchableTableColumn> getRecords() {
		return records;
	}

	public DefaultDataSet<MatchableTableColumn, MatchableTableColumn> getSchema() {
		return schema;
	}

	/**
	 * @return the candidateKeys
	 */
	public DefaultDataSet<MatchableTableKey, MatchableTableColumn> getCandidateKeys() {
		return candidateKeys;
	}
	
	public HashMap<Integer, MatchableTableColumn> getKeys() {
		return keys;
	}

	/**
	 * @return the tableRecords
	 */
	public DefaultDataSet<MatchableTable, MatchableTableColumn> getTableRecords() {
		return tableRecords;
	}
	
	/**
	 * A map (Column Identifier) -> (Column Header)
	 * @return
	 */
	public HashMap<String, String> getColumnHeaders() {
		return columnHeaders;
	}
	
	/**
	 * @return the tables
	 */
	public HashMap<Integer, Table> getTables() {
		return tables;
	}
	
	/**
	 * A map (Table Path) -> (Table Id)
	 * @return the tableIndices
	 */
	public HashMap<String, Integer> getTableIndices() {
		return tableIndices;
	}
	
	public static WebTables deserialise(File location) throws FileNotFoundException {
		System.out.println("Deserialising Web Tables");
		
        Kryo kryo = new Kryo();
        
        Input input = new Input(new FileInputStream(location));
        WebTables web = kryo.readObject(input, WebTables.class);
        input.close();
        
        return web;
	}

	public void serialise(File location) throws FileNotFoundException {
		System.out.println("Serialising Web Tables");
		
        Kryo kryo = new Kryo();
        Output output = new Output(new FileOutputStream(location));
        kryo.writeObject(output, this);
        output.close();
	}
	
	public static void writeTables(Collection<Table> tables, File jsonLocation, File csvLocation) throws IOException {
    	for(Table t : tables) {
    		if(jsonLocation!=null) {
		    	JsonTableWriter jtw = new JsonTableWriter();
				jtw.write(t, new File(jsonLocation, t.getPath()));
    		}
			
    		if(csvLocation!=null) {
				CSVTableWriter tw = new CSVTableWriter();
				tw.write(t, new File(csvLocation, t.getPath()));
    		}
    	}
	}
	
	public static void calculateDependenciesAndCandidateKeys(Collection<Table> tables, File csvTablesLocation) throws IOException, AlgorithmExecutionException {
		for(Table t : tables) {
			Set<TableColumn> dedupColumns = new HashSet<>();
			for(TableColumn c : t.getColumns()) {
				if(!SpecialColumns.isSpecialColumn(c)) {
					dedupColumns.add(c);
				}
			}
			// remove duplicate rows (otherwise UCC discovery does not work)
			t.deduplicate(dedupColumns);
			
			File f = new File(csvTablesLocation, t.getPath());
			CSVTableWriter tw = new CSVTableWriter();
			f = tw.write(t, f);
			
			Map<Collection<TableColumn>, Collection<TableColumn>> fd = FunctionalDependencyUtils.calculateFunctionalDependencies(t, f);
			t.getSchema().setFunctionalDependencies(fd);
			
			Collection<Set<TableColumn>> k = FunctionalDependencyUtils.calculateUniqueColumnCombinationsExcludingSpecialColumns(t, f);
			t.getSchema().setCandidateKeys(k);
		}
	}
}
