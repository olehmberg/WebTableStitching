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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import com.beust.jcommander.Parameter;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableContext;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableStatistics extends TnTTask {

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

	public static void main(String[] args) throws Exception {
		TableStatistics tu = new TableStatistics();

		if (tu.parseCommandLine(TableStatistics.class, args)) {

			tu.initialise();

			tu.match();

		}
	}

	private WebTables web;
	private File resultLocationFile;
	
	public void initialise() throws IOException {
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, true);

		// create output directory
		resultLocationFile = new File(resultLocation);
		resultLocationFile.mkdirs();

	}
	
	public void match() throws Exception {
		CSVWriter resultStatisticsWriter = new CSVWriter(new FileWriter(new File(resultLocationFile, "original.csv"), true));
		
		for(Table t : web.getTables().values()) {
			resultStatisticsWriter.writeNext(new String[] {
					new File(webLocation).getName(),
					t.getPath(),
					Integer.toString(t.getRows().size()),
					Integer.toString(t.getColumns().size())
			});
		}
		
		resultStatisticsWriter.close();
		
    	
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
