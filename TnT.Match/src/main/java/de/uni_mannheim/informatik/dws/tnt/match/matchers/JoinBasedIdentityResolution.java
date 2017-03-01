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

import java.util.Collection;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableMatchingKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.rules.refiner.SchemaCorrespondenceBasedEqualityMatchingRule;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.matching.blocking.RecordCorrespondenceBasedBlocker;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.Function;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class JoinBasedIdentityResolution {

	/***
	 * 
	 * Performs identity resolution on the provided tables using the join key (only columns from the join key are matched)
	 * requires pre-blocked instance correspondences
	 * assumes that records from t1 can be found in web
	 * 
	 * @param t1
	 * @param t2
	 * @param joinKey
	 * @param schemaMapping
	 * @param blockedInstanceCorrespondences
	 * @param web
	 * @param matchingEngine
	 * @param proc
	 * @return
	 */
	public ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> match(Table t1, Table t2, WebTableMatchingKey joinKey, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaMapping, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> blockedInstanceCorrespondences, WebTables web, MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, DataProcessingEngine proc) {
		/***********************************************
    	 * Identity Resolution
    	 ***********************************************/
		
		// run identity resolution with the schema correspondences
		SchemaCorrespondenceBasedEqualityMatchingRule irRule = new SchemaCorrespondenceBasedEqualityMatchingRule(1.0);
		
		// run the identity resolution on the chosen join key
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> keySchemaMapping = new ResultSet<>();
		final Collection<Integer> joinKeyT1Indices = Q.project(joinKey.getFirst(), new TableColumn.ColumnIndexProjection());
		final Collection<Integer> joinKeyT2Indices = Q.project(joinKey.getSecond(), new TableColumn.ColumnIndexProjection());
		
		// only use schema correspondences that are between the columns of the join key
		keySchemaMapping = proc.filter(schemaMapping, new Function<Boolean, Correspondence<MatchableTableColumn, MatchableTableRow>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean execute(
					Correspondence<MatchableTableColumn, MatchableTableRow> input) {
				return joinKeyT1Indices.contains(input.getFirstRecord().getColumnIndex()) || joinKeyT2Indices.contains(input.getSecondRecord().getColumnIndex());
			}});
		
		// as we only check key columns, missing values are not considered as correct matches
		irRule.setAllowMissingValues(false);
		
		// use instance correspondences from grouping step for blocking
		// (this identity resolution step only filters out incorrect correspondences, it cannot create new ones)
		RecordCorrespondenceBasedBlocker<MatchableTableRow, MatchableTableColumn> blocker = new RecordCorrespondenceBasedBlocker<>(blockedInstanceCorrespondences);
		
		// create the datasets for matching (of MatchableTableRows)
		DefaultDataSet<MatchableTableRow, MatchableTableColumn> dataT1 = new DefaultDataSet<>();
		int t1Id = web.getTableIndices().get(t1.getPath());
		for(TableRow r : t1.getRows()) {
			dataT1.add(new MatchableTableRow(r, t1Id));
		}
		DefaultDataSet<MatchableTableRow, MatchableTableColumn> dataT2 = new DefaultDataSet<>();
		int t2Id = web.getTableIndices().get(t2.getPath());
		for(TableRow r : t2.getRows()) {
			dataT2.add(new MatchableTableRow(r, t2Id));
		}
		
		for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : blockedInstanceCorrespondences.get()) {
			if(dataT1.getRecord(cor.getFirstRecord().getIdentifier())==null) {
//				TableRow r = t1.get(cor.getFirstRecord().getRowNumber());
				System.out.println("Missing record!");
			}
			if(dataT2.getRecord(cor.getSecondRecord().getIdentifier())==null) {
				System.out.println("Missing record!");
			}
		}
		
		return matchingEngine.runIdentityResolution(dataT1, dataT2, keySchemaMapping, irRule, blocker);
		
	}
	
}
