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

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.match.blocking.ClassAndTypeBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowDateComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.match.rules.DataTypeDependentSchemaMatchingRule;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.wdi.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.wdi.similarity.numeric.DeviationSimilarity;
import de.uni_mannheim.informatik.wdi.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DuplicateBasedSchemaMatching {

	private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
	private WebTables web;
	private KnowledgeBase kb;
	private SurfaceForms surfaceForms;
	private Map<Integer, Set<String>> classesPerTable;
	private boolean matchKeys;
	
	private ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences;
	/**
	 * @param instanceCorrespondences the instanceCorrespondences to set
	 */
	public void setInstanceCorrespondences(
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
		this.instanceCorrespondences = instanceCorrespondences;
	}
	
	private double valueSimilarityThreshold = 0.4;
	
	private double finalPropertySimilarityThreshold = 0.00; // was 0.03, but moved to CombineSchemaCorrespondences	
	/**
	 * @param finalPropertySimilarityThreshold the finalPropertySimilarityThreshold to set
	 */
	public void setFinalPropertySimilarityThreshold(double finalPropertySimilarityThreshold) {
		this.finalPropertySimilarityThreshold = finalPropertySimilarityThreshold;
	}
	
	private SimilarityMeasure<String> stringSimilarity = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5);
	
	private SimilarityMeasure<Double> numericSimilarity = new DeviationSimilarity();
	
//	private SimilarityMeasure<DateTime> dateSimilarity = new WeightedDateSimilarity(1, 3, 5);
	private WeightedDateSimilarity dateSimilarity = new WeightedDateSimilarity(1, 3, 5);
	
	private int numVotesPerValue = 0; // original T2K uses 2, but 0 seems to work better
	
	private int numCorrespondencesPerColumn = 3;
	
	private int numInstanceCandidates = 2;
	
	private double instanceCandidateThreshold = 0.5;
	
	public DuplicateBasedSchemaMatching(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, WebTables web, KnowledgeBase kb, SurfaceForms surfaceForms, Map<Integer, Set<String>> classesPerTable, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, boolean matchKeys) {
		this.matchingEngine = matchingEngine;
		this.web = web;
		this.kb = kb;
		this.surfaceForms = surfaceForms;
		this.classesPerTable = classesPerTable;
		this.instanceCorrespondences = instanceCorrespondences;
		this.matchKeys = matchKeys;
	}
	
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>  run() {
		DataProcessingEngine dataEngine = matchingEngine.getProcessingEngine();
		
		// select the top k candidates for each row and apply min similarity
		ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> bestCandidates = TopKMatch.getTopKMatch(instanceCorrespondences, dataEngine, numInstanceCandidates, instanceCandidateThreshold);
		
    	// create the duplicate-based schema matching rule
    	DataTypeDependentSchemaMatchingRule rule = new DataTypeDependentSchemaMatchingRule(finalPropertySimilarityThreshold);
    	rule.setComparatorForType(DataType.string, new MatchableTableRowComparatorBasedOnSurfaceForms(stringSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold, surfaceForms));
    	rule.setComparatorForType(DataType.numeric, new MatchableTableRowComparator<>(numericSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold)); // 0.4
//    	rule.setComparatorForType(DataType.date, new MatchableTableRowComparator<>(dateSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold));
    	rule.setComparatorForType(DataType.date, new MatchableTableRowDateComparator(dateSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold));
    	if(!matchKeys) {
    		rule.setRdfsLabelId(kb.getRdfsLabel().getColumnIndex());
    	}

    	// every value of the left-hand side has 2 votes for property correspondences (i.e. every lhs attribute creates up to two schema correspondences)
    	rule.setNumVotesPerValue(numVotesPerValue); 
    	// after aggregation, the best 3 schema correspondences for each attribute on the lhs are created
    	rule.setNumCorrespondences(numCorrespondencesPerColumn);
    
    	// create the blocker
    	ClassAndTypeBasedSchemaBlocker classAndTypeBasedSchemaBlocker = new ClassAndTypeBasedSchemaBlocker(kb, classesPerTable);
    	
    	// run matching
    	ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = matchingEngine.runDuplicateBasedSchemaMatching(web.getSchema(), kb.getSchema(), bestCandidates, rule, classAndTypeBasedSchemaBlocker);

    	// set similarity between keys and rdfs:label to keyWeight
    	// make matrix stochastic per table
    	// if final class already selected: set key sim of superclasses to key sim
    	
    	
    	return schemaCorrespondences;
	}
	
}
