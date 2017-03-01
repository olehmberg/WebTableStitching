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
package de.uni_mannheim.informatik.dws.tnt.match.rules.refiner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRuleWithVoting;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DuplicateBasedWebTableSchemaMatchingRule extends SchemaMatchingRuleWithVoting<MatchableTableRow, MatchableTableColumn, MatchableTableColumn> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<Integer, Map<Integer, String>> tableColumnIdentifiers;
	private boolean useLocalAmbiguityAvoidance = false;
	
	/**
	 * @return the tableColumnIdentifiers
	 */
	public Map<Integer, Map<Integer, String>> getTableColumnIdentifiers() {
		return tableColumnIdentifiers;
	}
	
	/**
	 * @param tableColumnIdentifiers the tableColumnIdentifiers to set
	 */
	public void setTableColumnIdentifiers(
			Map<Integer, Map<Integer, String>> tableColumnIdentifiers) {
		this.tableColumnIdentifiers = tableColumnIdentifiers;
	}
	
	/**
	 * Specifies whether local ambiguity avoidance should be applied.
	 * If the two records that are compared share a value that occurs at least twice in one of the records, the votes will be ambiguous between the two attributes with the same value for the same record.
	 * With local ambiguity avoidance, such records are not allowed to vote.
	 * 
	 * @param useLocalAmbiguityAvoidance the useLocalAmbiguityAvoidance to set
	 */
	public void setUseLocalAmbiguityAvoidance(boolean useLocalAmbiguityAvoidance) {
		this.useLocalAmbiguityAvoidance = useLocalAmbiguityAvoidance;
	}
	
	/**
	 * @return the useLocalAmbiguityAvoidance
	 */
	public boolean isUseLocalAmbiguityAvoidance() {
		return useLocalAmbiguityAvoidance;
	}
	
	/**
	 * @param finalThreshold
	 */
	public DuplicateBasedWebTableSchemaMatchingRule(double finalThreshold) {
		super(finalThreshold);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRule#apply(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
//	@Override
//	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> apply(
//			DataSet<MatchableTableColumn, MatchableTableColumn> schema1,
//			DataSet<MatchableTableColumn, MatchableTableColumn> schema2,
//			Correspondence<MatchableTableRow, MatchableTableColumn> correspondence) {
//		
//		MatchableTableRow record1 = correspondence.getFirstRecord();
//		MatchableTableRow record2 = correspondence.getSecondRecord();
//		
//		// make sure the schemas and correspondences have the same order
//		int t1Id = schema1.getRandomRecord().getTableId();
//		if(t1Id!=record1.getTableId()) {
//			MatchableTableRow tmp = record1;
//			record1 = record2;
//			record2 = tmp;
//		}
//
//		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> result = new ResultSet<>();
//		
//		if(isUseLocalAmbiguityAvoidance()) {
//			// collect all values of both records with their frequency
//			Distribution<Object> values1 = Distribution.fromCollection(Arrays.asList(record1.getValues()));
//			Distribution<Object> values2 = Distribution.fromCollection(Arrays.asList(record2.getValues()));
//			
//			// determine all values that occur in both records 
//			Set<Object> matchingValues = Q.intersection(values1.getElements(), values2.getElements());
//			
//			// check the frequency of the overlapping values
//			for(Object value : matchingValues) {
//				// if at least one record contains the value more than once
//				if(values1.getFrequency(value)>1 || values2.getFrequency(value)>1) {
//					// we have ambiguity and the records are not allowed to vote
//					return result;
//				}
//			}
//		}
//
//		// iterate over all values in record1
//		for(int i=0; i<record1.getRowLength(); i++) {
//			String col1Id = tableColumnIdentifiers.get(record1.getTableId()).get(i);
//			
//			boolean hasMatch = false;
//			
//			if(record1.hasColumn(i) && col1Id!=null) {
//				
//				// iterate over all values in record2
//				for(int j=0; j<record2.getRowLength(); j++) {
//					String col2Id = tableColumnIdentifiers.get(record2.getTableId()).get(j);
//					if(record2.hasColumn(j) && col2Id!=null) {
//						
//						// if the values are equal, vote for a schema correspondence
//						if(record1.get(i).equals(record2.get(j))) {
//							if(schema1.getRecord(col1Id)==null) {
//								System.out.println("\t" + StringUtils.join(Q.project(schema1.getRecords(), new MatchableTableColumn.ColumnIdProjection()), ",\n\t"));
//								System.out.println("Schema error!");
//							}
//							if(schema2.getRecord(col2Id)==null) {
//								System.out.println("\t" + StringUtils.join(Q.project(schema2.getRecords(), new MatchableTableColumn.ColumnIdProjection()), ",\n\t"));
//								System.out.println("Schema error!");
//							}
//							
//							Correspondence<MatchableTableColumn, MatchableTableRow> cor 
//							= new Correspondence<MatchableTableColumn, MatchableTableRow>(
//									schema1.getRecord(col1Id), 
//									schema2.getRecord(col2Id), 
//									1.0, 
//									null);
//							result.add(cor);
//							
//							if(hasMatch) {
//								System.out.println(String.format("!!! Second Vote for value %s", record1.get(i)));
//							}
//							hasMatch = true;
//						}
//					}
//				}
//			}
//		}
//		
//		return result;
//	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRule#aggregate(de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> aggregate(
			ResultSet<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>> results, int numVotes, DataProcessingEngine processingEngine) {

		SimilarityMatrix<MatchableTableColumn> cols = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
		Map<MatchableTableColumn, Map<MatchableTableColumn, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>>>> causes = new HashMap<>();
		
		for(Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>> p : results.get()) {
//			for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : p.getSecond().get()) {
			Correspondence<MatchableTableColumn, MatchableTableRow> cor = p.getSecond();
				Double currentScore = cols.get(cor.getFirstRecord(), cor.getSecondRecord());
				if(currentScore==null) {
					currentScore = 0.0;
				}
				cols.set(cor.getFirstRecord(), cor.getSecondRecord(), currentScore + cor.getSimilarityScore());
				cols.setLabel(cor.getFirstRecord(), cor.getFirstRecord().getColumnIndex());
				cols.setLabel(cor.getSecondRecord(), cor.getSecondRecord().getColumnIndex());
				
				Map<MatchableTableColumn, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>>> m = MapUtils.get(causes, cor.getFirstRecord(), new HashMap<MatchableTableColumn, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>>>());
				ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> causal = MapUtils.get(m, cor.getSecondRecord(), new ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>>());
				causal.add(p.getFirst());
//			}
		}
		
		System.out.println(cols.getOutput());
		
		cols.normalize(numVotes);;
		
		for(MatchableTableColumn col : cols.getFirstDimension()) {
			for(MatchableTableColumn col2 : cols.getMatches(col)) {
				if(cols.get(col, col2)<getFinalThreshold()) {
					cols.set(col, col2, null);
				}
			}
		}
		
		BestChoiceMatching bm = new BestChoiceMatching();
		bm.setForceOneToOneMapping(true);
		SimilarityMatrix<MatchableTableColumn> matched = bm.match(cols);
		
//		System.out.println(matched.getOutput());
		
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> result = new ResultSet<>();
		for(MatchableTableColumn col : matched.getFirstDimension()) {
			for(MatchableTableColumn col2 : matched.getMatches(col)) {
				Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(col, col2, matched.get(col, col2), causes.get(col).get(col2));
				result.add(cor);
			}
		}
		
		return result;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRuleWithVoting#apply(de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
	@Override
	public Correspondence<MatchableTableColumn, MatchableTableRow> apply(MatchableTableColumn schemaElement1,
			MatchableTableColumn schemaElement2,
			Correspondence<MatchableTableRow, MatchableTableColumn> correspondence) {
		MatchableTableRow record1 = correspondence.getFirstRecord();
		MatchableTableRow record2 = correspondence.getSecondRecord();
		
		// make sure the schemas and correspondences have the same order
		int t1Id = schemaElement1.getTableId();
		if(t1Id!=record1.getTableId()) {
			MatchableTableRow tmp = record1;
			record1 = record2;
			record2 = tmp;
		}

		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> result = new ResultSet<>();
		
		if(isUseLocalAmbiguityAvoidance()) {
			//TODO move this to the schema blocker
			// collect all values of both records with their frequency
			Distribution<Object> values1 = Distribution.fromCollection(Arrays.asList(record1.getValues()));
			Distribution<Object> values2 = Distribution.fromCollection(Arrays.asList(record2.getValues()));
			
			// determine all values that occur in both records 
			Set<Object> matchingValues = Q.intersection(values1.getElements(), values2.getElements());
			
			// check the frequency of the overlapping values
			for(Object value : matchingValues) {
				// if at least one record contains the value more than once
				if(values1.getFrequency(value)>1 || values2.getFrequency(value)>1) {
					// we have ambiguity and the records are not allowed to vote
					return null;
				}
			}
		}

		// iterate over all values in record1
//		for(int i=0; i<record1.getRowLength(); i++) {
			String col1Id = tableColumnIdentifiers.get(record1.getTableId()).get(schemaElement1.getColumnIndex());
			
//			boolean hasMatch = false;
			
			if(record1.hasColumn(schemaElement1.getColumnIndex()) && col1Id!=null) {
				
				// iterate over all values in record2
//				for(int j=0; j<record2.getRowLength(); j++) {
					String col2Id = tableColumnIdentifiers.get(record2.getTableId()).get(schemaElement2.getColumnIndex());
					if(record2.hasColumn(schemaElement2.getColumnIndex()) && col2Id!=null) {
						
						// if the values are equal, vote for a schema correspondence
						if(record1.get(schemaElement1.getColumnIndex()).equals(record2.get(schemaElement2.getColumnIndex()))) {
//							if(schema1.getRecord(col1Id)==null) {
//								System.out.println("\t" + StringUtils.join(Q.project(schema1.getRecords(), new MatchableTableColumn.ColumnIdProjection()), ",\n\t"));
//								System.out.println("Schema error!");
//							}
//							if(schema2.getRecord(col2Id)==null) {
//								System.out.println("\t" + StringUtils.join(Q.project(schema2.getRecords(), new MatchableTableColumn.ColumnIdProjection()), ",\n\t"));
//								System.out.println("Schema error!");
//							}
							
							Correspondence<MatchableTableColumn, MatchableTableRow> cor 
							= new Correspondence<MatchableTableColumn, MatchableTableRow>(
									schemaElement1, 
									schemaElement2, 
									1.0, 
									null);
//							result.add(cor);
							
							
//							if(hasMatch) {
//								System.out.println(String.format("!!! Second Vote for value %s", record1.get(schemaElement1.getColumnIndex())));
//							}
//							hasMatch = true;
							
							return cor;
						}
					}
				}
//			}
//		}
			return null;
	}


}
