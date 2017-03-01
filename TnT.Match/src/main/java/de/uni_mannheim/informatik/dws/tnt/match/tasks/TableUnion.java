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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import com.beust.jcommander.Parameter;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableContext;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.JsonTableWriter;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.wdi.utils.ProgressReporter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableUnion extends TnTTask {

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

	public static void main(String[] args) throws Exception {
		TableUnion tu = new TableUnion();

		if (tu.parseCommandLine(TableUnion.class, args)) {

			hello();

			tu.initialise();

			tu.match();

		}
	}

	private WebTables web;
	private File evaluationLocation;
	private File resultLocationFile;
	
	public void initialise() throws IOException {
		printHeadline("Table Union");
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, true);

		// create output directory
		resultLocationFile = new File(resultLocation);
		resultLocationFile.mkdirs();
		
		evaluationLocation = new File(new File(resultLocation), "evaluation");
    	evaluationLocation.mkdirs();
	}
	
	public void match() throws Exception {
		long start = System.currentTimeMillis();
		
    	/***********************************************
    	 * Schema Statistics
    	 ***********************************************/
    	// maps a table id to a schema
    	final HashMap<Integer, String> tableToSchema = new HashMap<>();
    	final HashMap<String, Integer> schemaToExtraColumns = new HashMap<>();
    	// maps a schema to a representative table for that schema
    	HashMap<String, Table> schemaToTable = new HashMap<>();
    	// let all the tables with the same schema vote for an entity label column (which we don't use, but it might be needed by other algorithms)
    	// schema -> (column index -> # votes)
    	Map<String, Map<Integer, Integer>> entityColumnVotes = new HashMap<>();
    	// keep statistics of original tables per schema
    	Map<String, Integer> originalTables = new HashMap<>();
    	Map<String, Integer> originalColumns = new HashMap<>();
    	
    	// the table schema statistics
    	TableSchemaStatistics stat = new TableSchemaStatistics();
    	
//    	int tableId = Q.max(web.getTables().keySet()) + 1;
    	int tableId = 0;
    	
    	/***********************************************
    	 * Context Columns
    	 ***********************************************/
    	// first iterate over all tables and collect the possible context attributes then add all these attributes to all tables
    	// needed to make sure all tables have the same schema w.r.t. the added columns
    	List<String> fragmentAttributes = new LinkedList<>();
//    	List<String> queryAttributes = new LinkedList<>();
    	for(Table t : web.getTables().values()) {
    		for(String s : getUriFragmentParts(t)) {
    			if(!fragmentAttributes.contains(s)) {
    				fragmentAttributes.add(s);
    			}
    		}
//    		for(String s : getUriQueryParts(t)) {
//    			if(!queryAttributes.contains(s)) {
//    				queryAttributes.add(s);
//    			}
//    		}
    	}
    	
    	// map the added context attributes to their relative column index
    	Map<String, Integer> contextAttributes = new HashMap<>();
    	for(int i=0; i<fragmentAttributes.size(); i++) {
    		contextAttributes.put(fragmentAttributes.get(i), i);
    	}
//    	for(int i=0; i<queryAttributes.size(); i++) {
//    		contextAttributes.put(queryAttributes.get(i), fragmentAttributes.size()+i);
//    	}
    	
    	/***********************************************
    	 * Create Union Tables (Schema)
    	 ***********************************************/
    	// sort the tables by file name to get reproducible table names for the output
    	List<Table> tables = new ArrayList<>(web.getTables().values());
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
    		String schema = String.format("%s", stat.generateSchemaString(t));
    		
    		// add the lookup from table id to schema
    		tableToSchema.put(web.getTableIndices().get(t.getPath()), schema);
    		
    		// add a representative table for that schema, if none exists yet
    		if(!schemaToTable.containsKey(schema)) {
    			// add a copy of the table as new union table
    			Table union = t.project(t.getColumns());
    			union.setTableId(tableId++);
    			union.setPath(Integer.toString(union.getTableId()));
    			schemaToTable.put(schema, union);
    			
    			TableContext ctx = new TableContext();
    			ctx.setTableNum(t.getContext().getTableNum());
    			URL url = new URL(t.getContext().getUrl());
    			ctx.setUrl(String.format("%s://%s", url.getProtocol(), url.getHost()));
    			union.setContext(ctx);
    			
    			// remove all rows from the union table
    			union.clear();
    			
    			int extraColumns = addUnionColumns(union, false, contextAttributes);
    			
    			schemaToExtraColumns.put(schema, extraColumns);

    			// vote for the entity label column (must be done *after* adding the context columns
    			Map<Integer, Integer> entityVotes = new HashMap<>();
    			entityColumnVotes.put(schema, entityVotes);
    			
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
    		}
    	}

    	// print the schema statistics to the console
    	stat.printSchemaList();
		
    	/***********************************************
    	 * Table Union
    	 ***********************************************/

    	File evaluationLocation = new File(new File(resultLocation), "evaluation");
    	evaluationLocation.mkdirs();
    	
    	ProgressReporter prg = new ProgressReporter(web.getTables().size(), "Creating Table Union");
    	// create the union of all tables with the same schema (ordered set of column headers)
		for(Table t : web.getTables().values()) {
			// get the schema of t
			String schema = tableToSchema.get(web.getTableIndices().get(t.getPath()));
			
			// get the table that represents t's schema (and which will be the union table)
			Table union = schemaToTable.get(schema);
			
			// add provenance information to the columns
			for(TableColumn c : union.getColumns()) {
				int extraColumns = schemaToExtraColumns.get(schema);
				
				if(!SpecialColumns.isSpecialColumn(c) && !ContextColumns.isContextColumn(c)) {
					TableColumn c2 = t.getSchema().get(c.getColumnIndex() - extraColumns);
					c.addProvenanceForColumn(c2);
				}
			}
			
			// count the original columns (without context columns)
			MapUtils.add(originalColumns, schema, t.getColumns().size());
			
			addUnionColumns(t, true, contextAttributes);
			
			// vote for the entity label column (must be done *after* adding the context columns to get the correct index)
			MapUtils.increment(entityColumnVotes.get(schema), t.getKeyIndex());
			
			// count the original tables
			MapUtils.increment(originalTables, schema);
			
			union.append(t);
			
			prg.incrementProgress();
			prg.report();
		}
		
		long duration = System.currentTimeMillis() -start;
		
    	/***********************************************
    	 * Write Results
    	 ***********************************************/
		File unionLocation = new File(new File(resultLocation), "union");
		setOutputDirectory(unionLocation);
		unionLocation.mkdirs();
		CSVWriter resultStatisticsWriter = new CSVWriter(new FileWriter(new File(resultLocationFile.getParent(), "union.csv"), true));
		JsonTableWriter jtw = new JsonTableWriter();
		for(Entry<String, Table> e : Q.sort(schemaToTable.entrySet(), new Comparator<Entry<String, Table>>() {

			@Override
			public int compare(Entry<String, Table> o1, Entry<String, Table> o2) {
				return Integer.compare(o1.getValue().getTableId(), o2.getValue().getTableId());
			}
		})) {
//		for(Table t : Q.sort(schemaToTable.values(), new Table.TableIdComparator())) {
			// set the entity label column with the majority of votes
//			String schema = tableToSchema.get(t.getTableId());
			String schema = e.getKey();
			Table t = e.getValue();
			
			t.setKeyIndex(MapUtils.max(entityColumnVotes.get(schema)));
			
			// write the table
			jtw.write(t, new File(unionLocation, t.getPath()));
			// write the statistics
			resultStatisticsWriter.writeNext(new String[] {
					resultLocationFile.getName(),
					t.getPath(),
					Integer.toString(t.getColumns().size()),
					Integer.toString(t.getRows().size()),
					stat.generateSchemaString(t),
					Integer.toString(schemaToExtraColumns.get(schema)),
					Integer.toString(originalTables.get(schema)),
					Integer.toString(originalColumns.get(schema)),
					Long.toString(duration)
			});
		}
		resultStatisticsWriter.close();
		
    	/***********************************************
    	 * Evaluation
    	 ***********************************************/
    	// write all provenance data for gold standard creation and evaluation
    	N2NGoldStandard n2n = new N2NGoldStandard();
		
		for(Table t : schemaToTable.values()) {
    		for(TableColumn c : t.getColumns()) {
    			Set<String> clu = new HashSet<>();
    			
    			if(!SpecialColumns.isSpecialColumn(c)) {
    				clu.addAll(c.getProvenance());
    			}
    			
    			n2n.getCorrespondenceClusters().put(clu, String.format("[%d]%s{%s}", c.getColumnIndex(), c.getHeader(), t.getPath()));
    		}
		}
		n2n.writeToTSV(new File(evaluationLocation, "correspondences_union.tsv"));
		
		// evaluate original t2k correspondences
		HashMap<String, Set<String>> t2kCorrespondences = new HashMap<>();
		for(Table t : web.getTables().values()) {
    		// get all the t2k correspondences
    		if(t.getMapping()!=null && t.getMapping().getMappedProperties()!=null) {
    			for(int idx = 0; idx <t.getMapping().getMappedProperties().length; idx++) {    				
    				if(t.getMapping().getMappedProperty(idx)!=null) {
	    				String prop = t.getMapping().getMappedProperty(idx).getFirst();
	    				TableColumn c = t.getSchema().get(idx+2);
	    				
	    				Set<String> cols = MapUtils.get(t2kCorrespondences, prop, new HashSet<String>());
	    				
	    				cols.add(c.getProvenanceString());
    				}
    			}
    		}
		}
    	N2NGoldStandard t2k = new N2NGoldStandard();
    	for(String prop : t2kCorrespondences.keySet()) {
    		Set<String> cols = t2kCorrespondences.get(prop);
    		
    		t2k.getCorrespondenceClusters().put(cols, prop);
    	}
    	t2k.writeToTSV(new File(evaluationLocation, "t2k_correspondences.tsv"));
    	
    	File gsFile = new File(evaluationLocation, "goldstandard_dbp.tsv");
    	if(gsFile.exists()) {
	    	N2NGoldStandard gs = new N2NGoldStandard();
	    	gs.loadFromTSV(gsFile);
	    	ClusteringPerformance perf = gs.evaluateCorrespondenceClusters(t2k.getCorrespondenceClusters(), false);
	    	System.out.println(gs.formatEvaluationResult(perf.getPerformanceByCluster(), false));
    	}
	}

	private List<String> getUriFragmentParts(Table t) throws URISyntaxException {
		URI u = new URI(t.getContext().getUrl());
		
		List<String> parts = Q.toList(u.getPath().split("/"));
		
		if(parts!=null && parts.size()>0) {
			parts.remove(0); // the path starts with /, so the first element is empty
			
			for(int i = 0; i < parts.size(); i++) {
				parts.set(i, ContextColumns.createUriPartHeader(i));
			}
		}
		
		return parts;
	}

	private List<String> getUriFragmentValues(Table t) throws URISyntaxException {
		URI u = new URI(t.getContext().getUrl());
		
		List<String> parts = Q.toList(u.getPath().split("/"));
		if(parts!=null && parts.size()>0) {
			parts.remove(0); // the path starts with /, so the first element is empty
			
			for(int i = 0; i < parts.size(); i++) {
				parts.set(i, parts.get(i));
			}
		}
		
		return parts;
	}
	
//	private List<String> getUriQueryParts(Table t) throws URISyntaxException {
//		URI u = new URI(t.getContext().getUrl());
//		
//		List<String> parts = new ArrayList<>();
//		
//		if(u.getQuery()!=null) {
//			
//			List<NameValuePair> params = URLEncodedUtils.parse(u, "UTF-8");
//			
//			Map<String, Integer> paramCounts = new HashMap<>();
//			
//			for(NameValuePair param : params) {
//				
//				int idx = MapUtils.increment(paramCounts, param.getName());
//				
//				if(idx>1) {
//					parts.add(String.format("%s[%d]", param.getName(), idx));
//				} else {
//					parts.add(param.getName());
//				}
//				
//			}
//		}
//		
//		for(int i = 0; i < parts.size(); i++) {
//			parts.set(i, ContextColumns.createUriQueryPartHeader(parts.get(i)));
//		}
//		
//		return parts;
//	}
//	
//	private List<String> getUriQueryValues(Table t) throws URISyntaxException {
//		URI u = new URI(t.getContext().getUrl());
//		
//		List<String> parts = new ArrayList<>(); 
//		
//		if(u.getQuery()!=null) {
//			
//			List<NameValuePair> params = URLEncodedUtils.parse(u, "UTF-8");
//			
//			for(NameValuePair param : params) {
//				parts.add(param.getValue());
//			}
//		}
//		
//		return parts;
//	}
	
	private int addUnionColumns(Table t, boolean addData, final Map<String, Integer> contextAttributes) throws URISyntaxException {
		int extraColumns = 0;
		
//		// create and fill the source table column
//		TableColumn tableIdColumn = new TableColumn(0, t);
//		tableIdColumn.setDataType(DataType.string);
//		tableIdColumn.setHeader(SpecialColumns.SOURCE_TABLE_COLUMN);
//		t.insertColumn(0, tableIdColumn);
//		extraColumns++;
//		
//		// add the row number as second context surrogate key
//		TableColumn rowNumberColumn = new TableColumn(1, t);
//		rowNumberColumn.setDataType(DataType.string);
//		rowNumberColumn.setHeader(SpecialColumns.ROW_NUMBER_COLUMN);
//		t.insertColumn(1, rowNumberColumn);
//		extraColumns++;
		
		// add columns for the table's context
		TableColumn titleColumn = new TableColumn(extraColumns, t);
		titleColumn.setDataType(DataType.string);
		titleColumn.setHeader(ContextColumns.PAGE_TITLE_COLUMN);
		t.insertColumn(extraColumns, titleColumn);
		extraColumns++;
		
		TableColumn headingColumn = new TableColumn(extraColumns, t);
		headingColumn.setDataType(DataType.string);
		headingColumn.setHeader(ContextColumns.TALBE_HEADING_COLUMN);
		t.insertColumn(extraColumns, headingColumn);
		extraColumns++;
		
		List<String> uriParts = getUriFragmentParts(t);
//		List<String> queryParts = getUriQueryParts(t);
//		
//		for(int i = 0; i < uriParts.size(); i++) {
//			TableColumn uriColumn = new TableColumn(4+i, t);
//			uriColumn.setDataType(DataType.string);
//			uriColumn.setHeader(ContextColumns.createUriPartHeader(i));
//			t.insertColumn(4+i, uriColumn);
//			extraColumns++;
//		}
//		
//		for(int i = 0; i < queryParts.size(); i++) {
//			TableColumn uriColumn = new TableColumn(4+i, t);
//			uriColumn.setDataType(DataType.string);
//			uriColumn.setHeader(ContextColumns.createUriQueryPartHeader(queryParts.get(i)));
//			t.insertColumn(4+uriParts.size()+i, uriColumn);
//			extraColumns++;
//		}
		
		List<String> ctxCols = Q.sort(contextAttributes.keySet(), new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return Integer.compare(contextAttributes.get(o1), contextAttributes.get(o2));
			}});
		
		for(int i =0; i<ctxCols.size();i++) {
			TableColumn uriColumn = new TableColumn(extraColumns, t);
			uriColumn.setDataType(DataType.string);
			uriColumn.setHeader(ctxCols.get(i));
			t.insertColumn(extraColumns, uriColumn);
			
			extraColumns++;
		}
		
		if(addData) {
			TableContext ctx = t.getContext();
		
//			List<String> queryValues = getUriQueryValues(t);
			List<String> uriValues = getUriFragmentValues(t);
			
			// fill the new columns with values
			for(TableRow r : t.getRows()) {
//				r.set(0, t.getPath());
				
				if(r.getRowNumber()>t.getSize()) {
					System.out.println("Incorrect Row Numbers!");
				}
				
//				r.set(1, Integer.toString(r.getRowNumber()));
				
				r.set(0, ctx.getPageTitle());
				r.set(1, ctx.getTableTitle());
				
				for(int i=0; i < uriParts.size(); i++) {
					int index = 2 + contextAttributes.get(uriParts.get(i));
					r.set(index,uriValues.get(i));
				}
				
//				for(int i=0; i < queryParts.size(); i++) {
//					int index = 2 + contextAttributes.get(queryParts.get(i));
//					r.set(4+index,queryValues.get(i));
//				}
			}
		}
		
		return extraColumns;
	}
}
