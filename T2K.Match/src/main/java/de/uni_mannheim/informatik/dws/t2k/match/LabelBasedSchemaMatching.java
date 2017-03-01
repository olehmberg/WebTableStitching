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
package de.uni_mannheim.informatik.dws.t2k.match;

import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.match.blocking.ClassAndTypeBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.SchemaLabelComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.wdi.matching.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class LabelBasedSchemaMatching {

	private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
	private WebTables web;
	private KnowledgeBase kb;
	private Map<Integer, Set<String>> classesPerTable;
	private ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences;
	
	/**
	 * @param instanceCorrespondences the instanceCorrespondences to set
	 */
	public void setInstanceCorrespondences(
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
		this.instanceCorrespondences = instanceCorrespondences;
	}
	
	public LabelBasedSchemaMatching(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, WebTables web, KnowledgeBase kb, Map<Integer, Set<String>> classesPerTable, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
		this.matchingEngine = matchingEngine;
		this.web = web;
		this.kb = kb;
		this.classesPerTable = classesPerTable;
		this.instanceCorrespondences = instanceCorrespondences;
	}
	
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> run() throws Exception {		
		// create the blocker
		ClassAndTypeBasedSchemaBlocker classAndTypeBasedSchemaBlocker = new ClassAndTypeBasedSchemaBlocker(kb, classesPerTable);
		
//		create the schema matching rule
		LinearCombinationMatchingRule<MatchableTableColumn, MatchableTableRow> lRule = new LinearCombinationMatchingRule<>(0.0);
		SchemaLabelComparator labelComparator = new SchemaLabelComparator(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5));
		lRule.addComparator(labelComparator, 1.0);

		return matchingEngine.runSchemaMatching(web.getSchema(), kb.getSchema(), instanceCorrespondences, lRule, classAndTypeBasedSchemaBlocker);
	}
	
}
