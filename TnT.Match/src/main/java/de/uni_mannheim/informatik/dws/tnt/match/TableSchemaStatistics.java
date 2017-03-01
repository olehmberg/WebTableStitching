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

import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.t2k.utils.cli.Executable;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableSchemaStatistics  extends Executable {

	@Parameter(names = "-web")
	private String webLocation;

	public static void main(String[] args) throws Exception {
		TableSchemaStatistics stat = new TableSchemaStatistics();

		if (stat.parseCommandLine(TableSchemaStatistics.class, args)) {
			System.out.println(String.format("Web tables location: %s", new File(stat.webLocation).getAbsolutePath()));
			
			WebTables web;

			web = WebTables.loadWebTables(new File(stat.webLocation), true, true, false, false);

			for(Table t : web.getTables().values()) {
				stat.addTable(t);
			}
			
			stat.print();
		}
	}


	private Map<String, Integer> schemaCounts;
	private Map<String, Map<String, Integer>> typedCounts;
	private Map<String, Map<String, Integer>> keyCounts;
	private Map<String, Map<String, Integer>> candidateKeyCounts;
	private Map<String, Integer> headerCounts;
	private Map<Integer, Integer> columnCountCounts;
	private Map<String, Integer> candidateKeyCountsTotal;
	
	/**
	 * @return the schemaCounts
	 */
	public Map<String, Integer> getSchemaCounts() {
		return schemaCounts;
	}
	
	/**
	 * @return the typedCounts
	 */
	public Map<String, Map<String, Integer>> getTypedCounts() {
		return typedCounts;
	}
	
	/**
	 * @return the keyCounts
	 */
	public Map<String, Map<String, Integer>> getKeyCounts() {
		return keyCounts;
	}
	
	/**
	 * @return the candidateKeyCounts
	 */
	public Map<String, Map<String, Integer>> getCandidateKeyCounts() {
		return candidateKeyCounts;
	}
	
	/**
	 * @return the candidateKeyCountsTotal
	 */
	public Map<String, Integer> getCandidateKeyCountsTotal() {
		return candidateKeyCountsTotal;
	}
	
	public TableSchemaStatistics() {
		schemaCounts = new HashMap<>();
		typedCounts = new HashMap<>();
		keyCounts = new HashMap<>();
		candidateKeyCounts = new HashMap<>();
		headerCounts = new HashMap<>();
		columnCountCounts = new HashMap<>();
		candidateKeyCountsTotal = new HashMap<>();
	}
	
	public void printSchemaList() {
		// sort by schema frequency descending
		List<Map.Entry<String, Integer>> sorted = MapUtils.sort(schemaCounts, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return -Integer.compare(o1.getValue(), o2.getValue());
			}
		});
		
		for(Map.Entry<String, Integer> entry : sorted) {
			System.out.println(String.format("%d\t%s",entry.getValue(), entry.getKey()));
		}
	}
	
	public void print() {
		// sort by schema frequency descending
		List<Map.Entry<String, Integer>> sorted = MapUtils.sort(schemaCounts, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return -Integer.compare(o1.getValue(), o2.getValue());
			}
		});
		
		for(Map.Entry<String, Integer> entry : sorted) {
			System.out.println(String.format("%d\t%s",entry.getValue(), entry.getKey()));
			
			/******* Keys by Key Detection */
			// sort by key frequency descending
			List<Map.Entry<String, Integer>> sortedKeys = MapUtils.sort(keyCounts.get(entry.getKey()), new Comparator<Map.Entry<String, Integer>>() {

				@Override
				public int compare(Entry<String, Integer> o1,
						Entry<String, Integer> o2) {
					return -Integer.compare(o1.getValue(), o2.getValue());
				}
			});
			System.out.println("\tDetected Keys:");
			for(Map.Entry<String, Integer> keyEntry : sortedKeys) {
				System.out.println(String.format("\t%d\t%s", keyEntry.getValue(), keyEntry.getKey()));
			}
			
			/******* Candidate Keys  */ 
			// sort by key frequency descending
			List<Map.Entry<String, Integer>> sortedCandidateKeys = MapUtils.sort(candidateKeyCounts.get(entry.getKey()), new Comparator<Map.Entry<String, Integer>>() {

				@Override
				public int compare(Entry<String, Integer> o1,
						Entry<String, Integer> o2) {
					return -Integer.compare(o1.getValue(), o2.getValue());
				}
			});
			System.out.println("\tCandidate Keys:");
			for(Map.Entry<String, Integer> keyEntry : sortedCandidateKeys) {
				System.out.println(String.format("\t%d\t%s", keyEntry.getValue(), keyEntry.getKey()));
			}
			
			/******* Data Types by Type Detection */
			// sort by type frequency descending
			List<Map.Entry<String, Integer>> sortedTypes = MapUtils.sort(typedCounts.get(entry.getKey()), new Comparator<Map.Entry<String, Integer>>() {

				@Override
				public int compare(Entry<String, Integer> o1,
						Entry<String, Integer> o2) {
					return -Integer.compare(o1.getValue(), o2.getValue());
				}
			});
			System.out.println("\tData Types:");
			for(Map.Entry<String, Integer> typeEntry : sortedTypes) {
				System.out.println(String.format("\t%d\t%s", typeEntry.getValue(), typeEntry.getKey()));
			}
		}
		
		/******* Candidate Keys - frequency distribution */
		System.out.println("Candidate Key distribution");
		System.out.println("\tf\tkey");
		sorted = MapUtils.sort(candidateKeyCountsTotal, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return -Integer.compare(o1.getValue(), o2.getValue());
			}
		});
		for(Map.Entry<String, Integer> entry : sorted) {
			System.out.println(String.format("\t%d\t%s", entry.getValue(), entry.getKey()));
		}
		
		/******* Column Headers - frequency distribution */
		// header frequencies
		System.out.println("Header distribution");
		System.out.println("\tf\theader");
		sorted = MapUtils.sort(headerCounts, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return -Integer.compare(o1.getValue(), o2.getValue());
			}
		});
		for(Map.Entry<String, Integer> entry : sorted) {
			System.out.println(String.format("\t%d\t%s", entry.getValue(), entry.getKey()));
		}
		
		/******* Schema Sizes - frequency distribution */
		// column count frequencies
		System.out.println("Column count frequencies");
		System.out.println("\tf\tschema size");
		List<Map.Entry<Integer, Integer>> sortedInt = MapUtils.sort(columnCountCounts, new Comparator<Map.Entry<Integer, Integer>>() {

			@Override
			public int compare(Entry<Integer, Integer> o1,
					Entry<Integer, Integer> o2) {
				return -Integer.compare(o1.getValue(), o2.getValue());
			}
		});
		for(Map.Entry<Integer, Integer> entry : sortedInt) {
			System.out.println(String.format("\t%d\t%s", entry.getValue(), entry.getKey()));
		}		
	}
	
	public void addTable(Table t) {
		// count schema frequency
		String schema = generateSchemaString(t);
		MapUtils.increment(schemaCounts, schema);
		
		// make sure a type map exists for this schema
		Map<String, Integer> schemaTypesCounts = MapUtils.get(typedCounts, schema, new HashMap<String, Integer>()); 
		
		// count typed schema frequency
		String schemaTypes = generateTypeString(t);
		MapUtils.increment(schemaTypesCounts, schemaTypes);
		
		// make sure a key map exists for this schema
		Map<String, Integer> schemaKeyCounts = MapUtils.get(keyCounts, schema, new HashMap<String, Integer>());
		
		// count key frequency
		if(t.getKey()!=null) {
			String key = t.getKey().getHeader();
			MapUtils.increment(schemaKeyCounts, key);
		}
		
		// count candidate key frequency
		if(t.getSchema().getCandidateKeys()!=null) {
			Map<String, Integer> candKeyMap = MapUtils.get(candidateKeyCounts, schema, new HashMap<String,Integer>());
			for(Collection<TableColumn> ck : t.getSchema().getCandidateKeys()) {
				String value = String.format("{%s}", StringUtils.join(Q.project(ck, new TableColumn.ColumnHeaderProjection()), ","));
				MapUtils.increment(candKeyMap, value);
				MapUtils.increment(candidateKeyCountsTotal, value);
			}
		}
		
		// count header frequency
		for(TableColumn c : t.getColumns()) {
			MapUtils.increment(headerCounts, c.getHeader());
		}
		
		// count column count
		MapUtils.increment(columnCountCounts, t.getColumns().size());
	}
	
	public String generateSchemaString(Table t) {
		StringBuilder sb = new StringBuilder();
		
		boolean first = true;
		
		//TODO add host to schema
		
		for(TableColumn col : t.getColumns()) {
			
			if(!first) {
				sb.append('+');
			}
			first = false;
			
			sb.append(col.getHeader());
		}
		
		return sb.toString();
	}
	
	public String generateNonContextSchemaString(Table t) {
		StringBuilder sb = new StringBuilder();
		
		boolean first = true;
		
		//TODO add host to schema
		
		for(TableColumn col : t.getColumns()) {
			
			if(!ContextColumns.isContextColumn(col)) {
				if(!first) {
					sb.append('+');
				}
				first = false;
				
				sb.append(col.getHeader());
			}
		}
		
		return sb.toString();
	}
	
	public String generateTypeString(Table t) {
		StringBuilder sb = new StringBuilder();
		
		boolean first = true;
		
		for(TableColumn col : t.getColumns()) {
			
			if(!first) {
				sb.append('+');
			}
			first = false;
			
//			sb.append(col.getHeader());
//			sb.append('/');
			sb.append(col.getDataType().toString());
		}
		
		return sb.toString();
	}
}
