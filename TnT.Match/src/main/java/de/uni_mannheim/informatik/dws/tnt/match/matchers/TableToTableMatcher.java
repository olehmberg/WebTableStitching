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

import java.util.HashSet;
import java.util.Set;

import check_if_useful.MatchingKeyGenerator;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableMatchingKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableToTableMatcher {

	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping = null;
	ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceMapping = null;
	WebTableMatchingKey matchingKey = null;
	Set<WebTableMatchingKey> matchingKeys = null;
	
	/**
	 * @return the schemaMapping
	 */
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> getSchemaMapping() {
		return schemaMapping;
	}
	/**
	 * @return the instanceMapping
	 */
	public ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> getInstanceMapping() {
		return instanceMapping;
	}
	/**
	 * @return the matchingKey
	 */
	public WebTableMatchingKey getMatchingKey() {
		return matchingKey;
	}
	
	/**
	 * @return the matchingKeys
	 */
	public Set<WebTableMatchingKey> getMatchingKeys() {
		return matchingKeys;
	}
	
	public void match(Table t1, Table t2, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> candidateInstanceCorrespondences,
			WebTables web, MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, DataProcessingEngine proc) {
		
		// perform duplicate-based schema matching between the tables using the instance correspondences created from schema-free identity resolution
		DuplicateBasedSchemaMatcher schemaMatcher = new DuplicateBasedSchemaMatcher();
		
		schemaMapping = schemaMatcher.match(t1, t2, web, matchingEngine, candidateInstanceCorrespondences, 0.0);
//		schemaMapping = schemaMatcher.match(t1, t2, web, matchingEngine, candidateInstanceCorrespondences, 1.0);
				
		for(Correspondence<MatchableTableColumn, MatchableTableRow> schemaCor : schemaMapping.get()) {
			
			if(schemaCor.getSimilarityScore()<1.0) {
				
				System.out.println(String.format("Uncertain Schema Correspondence (%.6f; %d votes): %s<->%s", schemaCor.getSimilarityScore(), candidateInstanceCorrespondences.size(), schemaCor.getFirstRecord(), schemaCor.getSecondRecord()));
				
				for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : schemaCor.getCausalCorrespondences().get()) {
					
					Object firstValue = cor.getFirstRecord().get(schemaCor.getFirstRecord().getColumnIndex());
					Object secondValue = cor.getSecondRecord().get(schemaCor.getSecondRecord().getColumnIndex());
					
					if(!firstValue.equals(secondValue)) {
						System.out.println(String.format("\t%s != %s", firstValue, secondValue));
					}
					
				}
				
			}
			
		}
		
		runIdentityResolution(t1, t2, candidateInstanceCorrespondences, schemaMapping, web, matchingEngine, proc);
		
//		// find a join key between the tables
//		MatchingKeyGenerator joinKeyGenerator = new MatchingKeyGenerator();
//		matchingKeys = joinKeyGenerator.generateAllJoinKeysFromCorrespondences(t1, t2, schemaMapping, 1.0);
//		
//		if(matchingKeys==null) {
//
//		} else {
//			matchingKey =  matchingKeys.iterator().next();
//			
//
//		}
//		
//		
//		if(matchingKey==null) {
//			// if there is no join key, we cannot trust any instance correspondence
//			// the connection between t1 and t2 is not valid
//		}
//		// only keep the connection between the tables if a valid join key exists!
//		else {
//    		// perform join-based identity resolution between the tables to filter out incorrect correspondences generated during schema-free identity resolution (based on the selected join key)
//    		JoinBasedIdentityResolution identityResolution = new JoinBasedIdentityResolution();
//    		instanceMapping = identityResolution.match(t1, t2, matchingKey, schemaMapping, candidateInstanceCorrespondences, web, matchingEngine, proc);
//		}
	}
	
	public void runIdentityResolution(Table t1, Table t2, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> candidateInstanceCorrespondences, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping, 
			WebTables web, MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, DataProcessingEngine proc) {

		this.schemaMapping = schemaMapping;

		// find a join key between the tables
		MatchingKeyGenerator joinKeyGenerator = new MatchingKeyGenerator();
		matchingKeys = joinKeyGenerator.generateAllJoinKeysFromCorrespondences(t1, t2, schemaMapping, 1.0);
		
		if(matchingKeys==null) {

			System.out.println(String.format("No matching key found! {#%d}%s<->{#%d}%s", t1.getTableId(), t1.getPath(), t2.getTableId(), t2.getPath()));
			System.out.println(String.format("Keys for %d", t1.getTableId()));
			for(Set<TableColumn> key : t1.getSchema().getCandidateKeys()) {
				System.out.println(String.format("\t{%s}", Q.project(key, new TableColumn.ColumnHeaderProjection())));
			}
			System.out.println(String.format("Keys for %d", t2.getTableId()));
			for(Set<TableColumn> key : t2.getSchema().getCandidateKeys()) {
				System.out.println(String.format("\t{%s}", Q.project(key, new TableColumn.ColumnHeaderProjection())));
			}
			
		} else {
			matchingKey =  matchingKeys.iterator().next();
			
			//TODO what happens if we do not use any matching key, but all of them?
			// - if the key is correct, nothing should change
			// - if the key is underestimated, we avoid incorrect instance correspondences
			// - if the key is too specific, we don't find any instance correspondences
			
			//TODO if they only key is underestimated (i.e. time for songs), then we will create incorrect instance correspondences 
			// - using those, we likely won't find any other schema correspondences
			// - this will lead to incorrect rows in the merged table (as both rows from the instance correspondence will be merged and don't create a conflict due to missing schema correspondences)
			
			
			Set<TableColumn> t1Columns = new HashSet<>();
			Set<TableColumn> t2Columns = new HashSet<>();
			
			for(WebTableMatchingKey p : matchingKeys) {
				t1Columns.addAll(p.getFirst());
				t2Columns.addAll(p.getSecond());
			}
			
			matchingKey = new WebTableMatchingKey(t1Columns, t2Columns);
		}
		
		
		if(matchingKey==null) {
			// if there is no join key, we cannot trust any instance correspondence
			// the connection between t1 and t2 is not valid
		}
		// only keep the connection between the tables if a valid join key exists!
		else {
			if(candidateInstanceCorrespondences!=null) {
	    		// perform join-based identity resolution between the tables to filter out incorrect correspondences generated during schema-free identity resolution (based on the selected join key)
	    		JoinBasedIdentityResolution identityResolution = new JoinBasedIdentityResolution();
	    		instanceMapping = identityResolution.match(t1, t2, matchingKey, schemaMapping, candidateInstanceCorrespondences, web, matchingEngine, proc);
			}
		}
	}
	
}
