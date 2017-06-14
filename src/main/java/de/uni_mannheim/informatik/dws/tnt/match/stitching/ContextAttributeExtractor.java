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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableContext;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * Extracts context attributes from the tables context data (page title, table heading, URL)
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ContextAttributeExtractor {


	public List<String> getUriFragmentParts(Table t) throws URISyntaxException {
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

	public List<String> getUriFragmentValues(Table t) throws URISyntaxException {
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
	
	public List<String> getUriQueryParts(Table t) throws URISyntaxException {
		URI u = new URI(t.getContext().getUrl());
		
		List<String> parts = new ArrayList<>();
		
		if(u.getQuery()!=null) {
			
			List<NameValuePair> params = URLEncodedUtils.parse(u, "UTF-8");
			
			Map<String, Integer> paramCounts = new HashMap<>();
			
			for(NameValuePair param : params) {
				
				int idx = MapUtils.increment(paramCounts, param.getName());
				
				if(idx>1) {
					parts.add(String.format("%s[%d]", param.getName(), idx));
				} else {
					parts.add(param.getName());
				}
				
			}
		}
		
		for(int i = 0; i < parts.size(); i++) {
			parts.set(i, ContextColumns.createUriQueryPartHeader(parts.get(i)));
		}
		
		return parts;
	}
	
	public List<String> getUriQueryValues(Table t) throws URISyntaxException {
		URI u = new URI(t.getContext().getUrl());
		
		List<String> parts = new ArrayList<>(); 
		
		if(u.getQuery()!=null) {
			
			List<NameValuePair> params = URLEncodedUtils.parse(u, "UTF-8");
			
			for(NameValuePair param : params) {
				parts.add(param.getValue());
			}
		}
		
		return parts;
	}
	
	public int addUnionColumns(Table t, boolean addData, final Map<String, Integer> contextAttributes) throws URISyntaxException {
		int extraColumns = 0;
		
		// add columns for the table's context
		List<String> ctxCols = Q.sort(contextAttributes.keySet(), new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return Integer.compare(contextAttributes.get(o1), contextAttributes.get(o2));
			}});
		
		Integer colIdx = null;
		for(int i =0; i<ctxCols.size();i++) {
			
			String header = ctxCols.get(i);
			colIdx = contextAttributes.get(header);
			
			TableColumn uriColumn = new TableColumn(colIdx, t);
			uriColumn.setDataType(DataType.string);
			uriColumn.setHeader(header);
			t.insertColumn(colIdx, uriColumn);
			
			extraColumns++;
		}
		
		if(addData) {
			TableContext ctx = t.getContext();
		
			List<String> uriParts = getUriFragmentParts(t);
			List<String> queryParts = getUriQueryParts(t);
			
			List<String> queryValues = getUriQueryValues(t);
			List<String> uriValues = getUriFragmentValues(t);
			
			// fill the new columns with values
			for(TableRow r : t.getRows()) {

				colIdx = contextAttributes.get(ContextColumns.PAGE_TITLE_COLUMN);
				if(colIdx!=null) {
					r.set(0, ctx.getPageTitle());
				}
				
				colIdx = contextAttributes.get(ContextColumns.TALBE_HEADING_COLUMN);
				if(colIdx!=null) {
					r.set(1, ctx.getTableTitle());
				}
				
				for(int i=0; i < uriParts.size(); i++) {
					String header = ContextColumns.createUriPartHeader(i);
					colIdx = contextAttributes.get(header);
					if(colIdx!=null) {
						r.set(colIdx,uriValues.get(i));
					}
				}
				
				for(int i=0; i < queryParts.size(); i++) {
					String header = ContextColumns.createUriQueryPartHeader(queryParts.get(i));
					colIdx = contextAttributes.get(header);
					if(colIdx!=null) {
						r.set(colIdx,queryValues.get(i));
					}
				}
			}
		}
		
		return extraColumns;
	}
	
}
