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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * A class that manages sets of disjoint headers, i.e., headers which cannot be matched to each other.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DisjointHeaders {

	/**
	 * 
	 */
	public DisjointHeaders(Map<String, Set<String>> disjointHeaders) {
		this.disjointHeaders = disjointHeaders;
		
		Iterator<String> headerIt = disjointHeaders.keySet().iterator();
		
		while(headerIt.hasNext()) {
			String header = headerIt.next();
			
			if(SpecialColumns.ALL.contains(header)) {
				headerIt.remove();
			} else {
				Iterator<String> disjointIt = disjointHeaders.get(header).iterator();
				
				while(disjointIt.hasNext()) {
					if(SpecialColumns.ALL.contains(disjointIt.next())) {
						disjointIt.remove();
					}
				}
			}
		}
	}
	
	public static DisjointHeaders fromTables(Collection<Table> tables) {
    	Map<String, Set<String>> disjointHeaders = new HashMap<>();
    	Map<String, Map<String, Integer>> disjointCounts = new HashMap<>();
    	Map<String, Map<String, Integer>> disjointCountsOriginal = new HashMap<>();
    	
    	// iterate over all tables
    	for(Table t : tables) {
    		// and their columns
    		for(int i = 0; i < t.getColumns().size(); i++) {
    			TableColumn c1 = t.getSchema().get(i);
    			
    			// consider the header if it is non-empty and not "null"
    			if(!c1.getHeader().equals("null") && !c1.getHeader().isEmpty()) {
    				
    				// get the set of already found, disjoint headers
    				Set<String> disjoint = MapUtils.get(disjointHeaders, c1.getHeader(), new HashSet<String>());
    				Map<String, Integer> counts = MapUtils.get(disjointCounts, c1.getHeader(), new HashMap<String, Integer>());
    				Map<String, Integer> countsOriginal = MapUtils.get(disjointCountsOriginal, c1.getHeader(), new HashMap<String, Integer>());
    				
    				// iterate over all other columns in the same table
	    			for(int j = 0; j < t.getColumns().size(); j++) {
	    				TableColumn c2 = t.getSchema().get(j);
	    				
//	    				if(i!=j && !c2.getHeader().equals("null") && !c2.getHeader().isEmpty()) {
	    				
	    				// a header is disjoint if it is non-empty, not "null", and not equal to the other header
	    				if(!c2.getHeader().equals("null") && !c2.getHeader().isEmpty() && !c1.getHeader().equals(c2.getHeader())) {
	    					disjoint.add(c2.getHeader());
	    					
	    					MapUtils.increment(counts, c2.getHeader());
	    					
	    					if(c1.getProvenance()!=null && c2.getProvenance()!=null) {
	    						MapUtils.add(countsOriginal, c2.getHeader(), Math.min(c1.getProvenance().size(), c2.getProvenance().size()));
	    					}
	    				}
	    			}
    			}
    		}
    	}
    	
    	DisjointHeaders dh = new DisjointHeaders(disjointHeaders);
    	dh.setDisjointCounts(disjointCounts);
    	dh.setDisjointCountsOriginal(disjointCountsOriginal);
    	return dh;
	}
	
	public Set<String> getDisjointHeaders(String header) {
		if(header==null || header.equals("null") || !disjointHeaders.containsKey(header)) {
			return new HashSet<>();
		} else {
			return disjointHeaders.get(header);
		}
	}
	
	Map<String, Set<String>> disjointHeaders = new HashMap<>();
	Map<String, Map<String, Integer>> disjointCounts = new HashMap<>();
	Map<String, Map<String, Integer>> disjointCountsOriginal = new HashMap<>();
	
	/**
	 * @return the disjointCounts
	 */
	public Map<String, Map<String, Integer>> getDisjointCounts() {
		return disjointCounts;
	}
	/**
	 * @param disjointCounts the disjointCounts to set
	 */
	protected void setDisjointCounts(Map<String, Map<String, Integer>> disjointCounts) {
		this.disjointCounts = disjointCounts;
	}
	/**
	 * @return the disjointCountsOriginal
	 */
	public Map<String, Map<String, Integer>> getDisjointCountsOriginal() {
		return disjointCountsOriginal;
	}
	/**
	 * @param disjointCountsOriginal the disjointCountsOriginal to set
	 */
	protected void setDisjointCountsOriginal(Map<String, Map<String, Integer>> disjointCountsOriginal) {
		this.disjointCountsOriginal = disjointCountsOriginal;
	}
	
	
	public Map<String, Set<String>> getAllDisjointHeaders() {
		return disjointHeaders;
	}
	
	public void extendWithSynonyms(Collection<Set<String>> synonyms) {
	
		Map<String, Collection<String>> attributeNameIndex = new HashMap<>();
		
		// filter attribute names: remove empty names
		// and create an index of attribute names
		for(Set<String> clu : synonyms) {
			Iterator<String> it = clu.iterator();
			
			while(it.hasNext()) {
				String attributeName = it.next();
				
				if(attributeName.equals("null")) {
					it.remove();
				} else {
					attributeNameIndex.put(attributeName, clu);
				}
			}
		}
		
		// update disjoint headers: add all synonyms to the disjoint headers
		HashMap<String, Set<String>> newDisjointHeaders = new HashMap<>();
		for(String header : disjointHeaders.keySet()) {
			
			Set<String> disjoint = new HashSet<>();
			
			for(String old : disjointHeaders.get(header)) {
				// for each existing header
				disjoint.add(old);
				
				if(attributeNameIndex.containsKey(old)) {
					// add all synonyms
					disjoint.addAll(attributeNameIndex.get(old));
				}
			}
			
			// add the new disjoint headers for all synonyms of the current header
			newDisjointHeaders.put(header, disjoint);
			if(attributeNameIndex.containsKey(header)) {
				// if there are synonyms for header
				for(String synonym : attributeNameIndex.get(header)) {
					newDisjointHeaders.put(synonym, disjoint);
				}
			}
		}
		disjointHeaders = newDisjointHeaders;
	}
	
	public String format() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Disjoint Headers:\n");
		
		for(String s : disjointHeaders.keySet()) {
			sb.append(String.format("%s\t%s\n", s, StringUtils.join(disjointHeaders.get(s), ",")));
		}
		
		return sb.toString();
	}
}
